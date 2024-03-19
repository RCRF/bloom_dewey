import sys
import jwt
import httpx
import os
import json
import uvicorn

# The following three lines allow for dropping embed() in to block and present an IPython shell
from IPython import embed
import nest_asyncio
nest_asyncio.apply()


from fastapi import (
    FastAPI,
    Depends,
    HTTPException,
    status,
    Request,
    Response,
    Form,
    Query,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import APIKeyCookie
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from starlette.responses import JSONResponse
from starlette.middleware.sessions import SessionMiddleware

from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy import func, text

from jinja2 import Environment, FileSystemLoader
from collections import defaultdict
from datetime import datetime, timedelta

from bloom_lims.bdb import BLOOMdb3
from bloom_lims.bdb import BloomObj, BloomWorkflow, BloomWorkflowStep
from auth.supabase.connection import create_supabase_client


# Initialize Jinja2 environment
templates = Environment(loader=FileSystemLoader("templates"))

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
app.mount("/templates", StaticFiles(directory="templates"), name="templates")

# Setup CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(SessionMiddleware, secret_key='your-secret-key')
# Serve static files
cookie_scheme = APIKeyCookie(name="session")
SKIP_AUTH = False if len(sys.argv) < 3 else True


class AuthenticationRequiredException(HTTPException):
    def __init__(self, detail: str = "Authentication required"):
        super().__init__(status_code=401, detail=detail)

async def is_instance(value, type_name):
    return isinstance(value, eval(type_name))


def get_well_color(quant_value):
    # Transition from purple to white
    if quant_value <= 0.5:
        r = int(128 + 127 * (quant_value / 0.5))  # From 128 to 255
        g = int(0 + 255 * (quant_value / 0.5))  # From 0 to 255
        b = int(128 + 127 * (quant_value / 0.5))  # From 128 to 255
    # Transition from white to green
    else:
        r = int(255 - 255 * ((quant_value - 0.5) / 0.5))  # From 255 to 0
        g = 255
        b = int(255 - 255 * ((quant_value - 0.5) / 0.5))  # From 255 to 0

    return f"rgb({r}, {g}, {b})"


async def get_relationship_data(obj):
    relationship_data = {}
    for relationship in obj.__mapper__.relationships:
        if relationship.uselist:  # If it's a list of items
            relationship_data[relationship.key] = [
                {
                    "child_instance_euid": rel_obj.child_instance.euid
                    if hasattr(rel_obj, "child_instance")
                    else [],
                    "parent_instance_euid": rel_obj.parent_instance.euid
                    if hasattr(rel_obj, "parent_instance")
                    else [],
                    "euid": rel_obj.euid,
                    "uuid": rel_obj.uuid,
                    "polymorphic_discriminator": rel_obj.polymorphic_discriminator,
                    "super_type": rel_obj.super_type,
                    "btype": rel_obj.btype,
                    "b_sub_type": rel_obj.b_sub_type,
                    "version": rel_obj.version,
                }
                for rel_obj in getattr(obj, relationship.key)
            ]
        else:  # If it's a single item
            rel_obj = getattr(obj, relationship.key)
            relationship_data[relationship.key] = [
                {
                    "child_instance_euid": rel_obj.child_instance.euid
                    if hasattr(rel_obj, "child_instance")
                    else [],
                    "parent_instance_euid": rel_obj.parent_instance.euid
                    if hasattr(rel_obj, "parent_instance")
                    else [],
                    "euid": rel_obj.euid,
                    "uuid": rel_obj.uuid,
                    "polymorphic_discriminator": rel_obj.polymorphic_discriminator,
                    "super_type": rel_obj.super_type,
                    "btype": rel_obj.btype,
                    "b_sub_type": rel_obj.b_sub_type,
                    "version": rel_obj.version,
                }
                if rel_obj
                else {}
            ]
    return relationship_data


class RequireAuthException(HTTPException):
    def __init__(self, detail: str):
        super().__init__(status_code=403, detail=detail)

@app.exception_handler(AuthenticationRequiredException)
async def authentication_required_exception_handler(request: Request, exc: AuthenticationRequiredException):
    return RedirectResponse(url="/login")

async def require_auth(request: Request):
    # Bypass auth check for the home page

    if request.url.path == '/':
        return {"email": "anonymous@user.com"}  # Return a default user or any placeholder

    if 'user_data' not in request.session:
        raise AuthenticationRequiredException()
    return request.session['user_data']


@app.exception_handler(RequireAuthException)
async def auth_exception_handler(_request: Request, _exc: RequireAuthException):
    # Redirect the user to the login page
    return RedirectResponse(url="/login")


@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request, _=Depends(require_auth)):
    count = request.session.get('count', 0)
    count += 1
    request.session['count'] = count
    
    template = templates.get_template("index.html")
    user_data = request.session.get('user_data', {})
    style = {"skin_css": user_data.get('style_css', 'static/skins/bloom.css')}
    context = {"request": request, "style": style}

    return HTMLResponse(content=template.render(context), status_code=200)


@app.get("/login", include_in_schema=False)
async def get_login_page(request: Request):
    user_data = request.session.get('user_data', {})
    style = {"skin_css": user_data.get('style_css', 'static/skins/bloom.css')}

    # Ensure you have this function defined, and it returns the expected style information
    template = templates.get_template("login.html")
    # Pass the 'style' variable in the context
    context = {"request": request, "style": style}
    return HTMLResponse(content=template.render(context))


@app.post("/oauth_callback")
async def oauth_callback(request: Request):
    body = await request.json()
    access_token = body.get('accessToken')

    if not access_token:
        return "No access token provided."
    # Attempt to decode the JWT to get email
    try:
        decoded_token = jwt.decode(access_token, options={"verify_signature": False})
        primary_email = decoded_token.get('email')
    except jwt.DecodeError:
        primary_email = None

    # Fetch user email from GitHub if not present in decoded token
    if not primary_email:
        async with httpx.AsyncClient() as client:
            headers = {"Authorization": f"Bearer {access_token}"}
            response = await client.get('https://api.github.com/user/emails', headers=headers)
            if response.status_code == 200:
                emails = response.json()
                primary_email = next((email['email'] for email in emails if email.get('primary')), None)
            else:
                raise HTTPException(status_code=400, detail="Failed to retrieve user email from GitHub")

    # Update user data file
    user_data_file = './etc/udat.json'
    os.makedirs(os.path.dirname(user_data_file), exist_ok=True)

    if not os.path.exists(user_data_file):
        with open(user_data_file, 'w') as f:
            json.dump({}, f)

    with open(user_data_file, 'r+') as f:
        user_data = json.load(f)
        user_data[primary_email] = {"style_css": "static/skins/bloom.css"}
        f.seek(0)
        json.dump(user_data, f, indent=4)
        f.truncate()
    
    request.session['user_data'] = {"email": primary_email, "style_css": "static/skins/bloom.css"}

    # Redirect to home page or dashboard
    return RedirectResponse(url="/", status_code=303)


@app.get("/logout")  # Using a GET request for simplicity, but POST is more secure for logout operations
async def logout(request: Request, response: Response):
    # Clear the session data
    request.session.clear()

    # Optionally, clear the session cookie.
    # Note: This might not be necessary if your session middleware automatically handles it upon session.clear().
    response.delete_cookie(key="session", path="/")

    # Redirect to the homepage
    return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/login", include_in_schema=False)
async def login_without_network(request: Request, response: Response, email: str = Form(...)):
    if not email:
        return JSONResponse(content={"message": "Email is required"}, status_code=status.HTTP_400_BAD_REQUEST)

    # Path to the user data file
    user_data_file = './etc/udat.json'

    # Ensure the directory exists
    os.makedirs(os.path.dirname(user_data_file), exist_ok=True)

    # Load or initialize the user data
    if not os.path.exists(user_data_file):
        user_data = {}
    else:
        with open(user_data_file, 'r') as f:
            user_data = json.load(f)

    # Check if the user exists in udat.json, sign up/login as necessary
    if email not in user_data:
        # Add the user if they don't exist

        user_data[email] = {"style_css": "static/skins/bloom.css"}
        with open(user_data_file, 'w') as f:
            json.dump(user_data, f, indent=4)
    # At this point, the user is considered logged in whether they were just added or already existed

    # Set session cookie after successful login, with a 60-minute expiration
    response.set_cookie(key="session", value="user_session_token", httponly=True, max_age=3600, path="/")
    request.session['user_data'] = {'email': email}
    print(request.session)

    # Redirect to the root path ("/") after successful login
    return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/login", include_in_schema=False)
async def login(request: Request, response: Response, email: str = Form(...)):
    # Use a static password for simplicity (not recommended for production)
    password = "notapplicable"
    # Initialize the Supabase client
    supabase = create_supabase_client()

    if not email:
        return JSONResponse(content={"message": "Email is required"}, status_code=status.HTTP_400_BAD_REQUEST)

    # Check if the user exists in udat.json
    user_data_file = './etc/udat.json'
    if not os.path.exists(user_data_file):
        os.makedirs(os.path.dirname(user_data_file), exist_ok=True)
        with open(user_data_file, 'w') as f:
            json.dump({}, f)

    with open(user_data_file, 'r+') as f:
        user_data = json.load(f)
        if email not in user_data:
            # The email is not in udat.json, attempt to sign up the user
            auth_response = supabase.auth.sign_up({"email": email, "password": password})
            if 'error' in auth_response and auth_response['error']:
                # Handle signup error
                return JSONResponse(content={"message": auth_response['error']['message']},
                                    status_code=status.HTTP_400_BAD_REQUEST)
            else:
                # Update udat.json with the new user
                user_data[email] = {"style_css": "static/skins/bloom.css"}
                f.seek(0)
                json.dump(user_data, f, indent=4)
                f.truncate()
        else:
            # The email exists in udat.json, attempt to sign in the user
            auth_response = supabase.auth.sign_in_with_password({"email": email, "password": password})
            if 'error' in auth_response and auth_response['error']:
                # Handle sign-in error
                return JSONResponse(content={"message": auth_response['error']['message']},
                                    status_code=status.HTTP_400_BAD_REQUEST)

    # Set session cookie after successful authentication, with a 60-minute expiration
    response.set_cookie(key="session", value="user_session_token", httponly=True, max_age=3600, path="/")
    request.session['user_data'] = {'email': email}
    # Redirect to the root path ("/") after successful login/signup
    return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)
    # Add this line at the end of the /login endpoint


templates = Environment(loader=FileSystemLoader("templates"))


@app.get("/assays", response_class=HTMLResponse)
async def assays(request: Request, show_type: str = 'all', _auth=Depends(require_auth)):
    # Check if user is logged in
    if 'user_data' not in request.session or 'email' not in request.session['user_data']:
        # If not logged in, redirect to the login page
        return RedirectResponse(url="/login")

    user_email = request.session['user_data']['email']
    user_data = request.session.get('user_data', {})

    # Initialize your database object with the user's email
    bobdb = BloomObj(BLOOMdb3(app_username=user_email))
    ay_ds = {}
    print('\n\n\nAAAAAAAA\n\n\n')
    for i in (
            bobdb.session.query(bobdb.Base.classes.workflow_instance)
                    .filter_by(is_deleted=False, is_singleton=True)
                    .all()
    ):
        if show_type == 'all' or i.json_addl.get('assay_type', 'all') == show_type:
            ay_ds[i.euid] = i

    print('\n\n\n\n\nBBBBBB\n\n\n\n')
    assays = []
    ay_dss = {}
    atype = {}

    if show_type == 'assay':
        atype['type'] = 'Assays'
    elif show_type == 'accessioning':
        atype['type'] = 'Accessioning'
    else:
        atype['type'] = 'All Assays, etc'

    for i in sorted(ay_ds.keys()):
        assays.append(ay_ds[i])
        ay_dss[i] = {"Instantaneous COGS": 0}  # round(bobdb.get_cost_of_euid_children(i),2)}
        ay_dss[i]['tot'] = 0
        ay_dss[i]['tit_s'] = 0
        ay_dss[i]['tot_fx'] = 0

        for q in ay_ds[i].parent_of_lineages:
            if show_type == 'accessioning':
                for fex_tup in bobdb.query_all_fedex_transit_times_by_ay_euid(q.child_instance.euid):
                    ay_dss[i]['tit_s'] += fex_tup[1]
                    ay_dss[i]['tot_fx'] += 1
            wset = ''
            n = q.child_instance.json_addl['properties']['name']
            if n.startswith('In'):
                wset = 'inprog'
            elif n.startswith('Comple'):
                wset = 'complete'
            elif n.startswith('Exception'):
                wset = 'exception'
            elif n.startswith('Ready'):
                wset = 'avail'
            lins = q.child_instance.parent_of_lineages.all()
            ay_dss[i][wset] = len(lins)
            lctr = 0
            lctr_max = 150
            for llin in lins:
                if lctr > lctr_max:
                    break
                else:
                    ay_dss[i]["Instantaneous COGS"] += round(
                        bobdb.get_cost_of_euid_children(llin.child_instance.euid), 2)
                    ay_dss[i]['tot'] += 1
                lctr += 1

        try:
            ay_dss[i]['avg_d_fx'] = round(
                float(ay_dss[i]['tit_s']) / 60.0 / 60.0 / 24.0 / float(ay_dss[i]['tot_fx']), 2)
        except Exception as e:
            ay_dss[i]['avg_d_fx'] = 'na'

        ay_dss[i]['conv'] = round(
            float(ay_dss[i]['complete']) / float(ay_dss[i]['complete'] + ay_dss[i]['exception']), 2) if ay_dss[i][
                                                                                                            'complete'] + \
                                                                                                        ay_dss[i][
                                                                                                            'exception'] > 0 else 'na'
        ay_dss[i]['wsetp'] = round(float(ay_dss[i]["Instantaneous COGS"]) / float(ay_dss[i]['tot']), 2) if \
            ay_dss[i]['tot'] > 0 else 'na'
    style = {"skin_css": user_data.get('style_css', 'static/skins/bloom.css')}

    # Rendering the template with the dynamic content
    content = templates.get_template("assay.html").render(
        style=style,
        user_logged_in=True,
        assays_data=ay_ds,
        atype=atype,
        workflow_instances=assays,  # Assuming this is needed based on your template logic
        ay_stats=ay_dss  # Assuming this is needed based on your template logic
    )

    return HTMLResponse(content=content)


@app.get("/calculate_cogs_children")
async def Acalculate_cogs_children(euid, request: Request, _auth=Depends(require_auth)):
    try:

        bobdb = BloomObj(BLOOMdb3(app_username=request.session['user_data']))
        cogs_value = round(bobdb.get_cost_of_euid_children(euid), 2)
        return json.dumps({"success": True, "cogs_value": cogs_value})
    except Exception as e:
        return json.dumps({"success": False, "message": str(e)})


async def calculate_cogs_parents(euid, request: Request, _auth=Depends(require_auth)):
    try:
        bobdb = BloomObj(BLOOMdb3(app_username=request.session['user_data']))
        cogs_value = round(bobdb.get_cogs_to_produce_euid(euid), 2)
        return json.dumps({"success": True, "cogs_value": cogs_value})
    except Exception as e:
        return json.dumps({"success": False, "message": str(e)})

@app.get("/set_filter")
async def set_filter(request: Request, _auth=Depends(require_auth), curr_val='off'):
    if curr_val == 'off':
        request.session['user_data']['wf_filter'] = 'on'
    else:
        request.session['user_data']['wf_filter'] = 'off'


@app.get("/admin", response_class=HTMLResponse)
async def admin(request: Request, _auth=Depends(require_auth), dest='na'):
    dest_section = {'section': dest}

    user_data = request.session.get('user_data', {})

    bobdb = BloomObj(BLOOMdb3(app_username=request.session['user_data']['email']))

    # Mock or real printer_info data

    if 'print_lab' in user_data:
        bobdb.get_lab_printers(user_data['print_lab'])

    csss = []
    for css in sorted(os.popen('ls -1 static/skins/*css').readlines()):
        csss.append(css.rstrip())

    printer_info = {
        'print_lab': bobdb.printer_labs,
        'printer_name': bobdb.site_printers,
        'label_zpl_style': bobdb.zpl_label_styles,
        'style_css': csss
    }
    csss = [os.path.basename(css) for css in csss]  # Get just the file names

    printer_info['style_css'] = csss
    style = {"skin_css": user_data.get('style_css', 'static/skins/bloom.css')}

    # Rendering the template with the dynamic content
    content = templates.get_template("admin.html").render(
        style=style,
        user_logged_in=True,
        user_data=user_data,
        printer_info=printer_info,
        dest_section=dest_section,
    )
    return HTMLResponse(content=content)


# Take a look at this later
@app.post("/update_preference")
async def update_preference(request: Request, auth: dict = Depends(require_auth)):
    # Early return if auth is None or doesn't contain 'email'
    if not auth or 'email' not in auth:
        return {'status': 'error', 'message': 'Authentication failed or user data missing'}

    data = await request.json()
    key = data.get('key')
    value = data.get('value')

    user_data_file = './etc/udat.json'
    if not os.path.exists(user_data_file):
        return {'status': 'error', 'message': 'User data file not found'}

    with open(user_data_file, 'r') as f:
        user_data = json.load(f)

    email = request.session.get('user_data', {}).get('email')
    if email in user_data:
        user_data[email][key] = value
        with open(user_data_file, 'w') as f:
            json.dump(user_data, f, indent=4)
        # Only update the style_css in session if the key is 'style_css'
        #if key == 'style_css':
        request.session['user_data'][key] = value
        return {'status': 'success', 'message': 'User preference updated'}
    else:
        return {'status': 'error', 'message': 'User not found in user data'}


@app.get("/queue_details", response_class=HTMLResponse)
async def queue_details(request: Request, queue_euid, page=1, _auth=Depends(require_auth)):
    page = int(page)
    if page < 1:
        page = 1
    per_page = 500  # Items per page
    user_logged_in = True if 'user_data' in request.session else False
    bobdb = BloomObj(BLOOMdb3(app_username=request.session['user_data']['email']))
    queue = bobdb.get_by_euid(queue_euid)
    qm = []
    for i in queue.parent_of_lineages:
        qm.append(i.child_instance)
    queue_details = queue.sort_by_euid(qm)
    queue_details = queue_details[(page - 1) * per_page:page * per_page]
    pagination = {'next': page + 1, 'prev': page - 1, 'euid': queue_euid}
    user_data = request.session.get('user_data', {})
    style = {"skin_css": user_data.get('style_css', 'static/skins/bloom.css')}

    content = templates.get_template("queue_details.html").render(
        style=style,
        queue=queue,
        queue_details=queue_details,
        pagination=pagination,
        user_logged_in=user_logged_in,
    )
    return HTMLResponse(content=content)


@app.post("/generic_templates")
async def generic_templates(request: Request, _auth=Depends(require_auth)):
    bobdb = BloomObj(BLOOMdb3(app_username=request.session['user_data']['email']))

    templates = (
        bobdb.session.query(bobdb.Base.classes.generic_template)
        .filter_by(is_deleted=False)
        .all()
    )

    # Group templates by super_type
    grouped_templates = {}
    for temp in templates:
        if temp.super_type not in grouped_templates:
            grouped_templates[temp.super_type] = []
        grouped_templates[temp.super_type].append(temp)
    return HTMLResponse(grouped_templates)


@app.get("/workflow_summary", response_class=HTMLResponse)
async def workflow_summary(request: Request, _auth=Depends(require_auth)):
    bobdb = BloomObj(BLOOMdb3(app_username=request.session['user_data']['email']))
    workflows = (
        bobdb.session.query(bobdb.Base.classes.workflow_instance)
        .filter_by(is_deleted=False)
        .all()
    )

    workflow_statistics = defaultdict(
        lambda: {
            "status_counts": defaultdict(int),
            "oldest": datetime.max.date(),
            "newest": datetime.min.date(),
        }
    )

    for wf in workflows:
        wf_type = wf.btype
        wf_status = wf.bstatus
        wf_created_dt = wf.created_dt.date()

        stats = workflow_statistics[wf_type]
        stats["status_counts"][wf_status] += 1
        stats["oldest"] = min(stats["oldest"], wf_created_dt)
        stats["newest"] = max(stats["newest"], wf_created_dt)

    workflow_statistics = {k: dict(v) for k, v in workflow_statistics.items()}
    unique_workflow_types = list(workflow_statistics.keys())

    user_data = request.session.get('user_data', {})
    style = {"skin_css": user_data.get('style_css', 'static/skins/bloom.css')}

    content = templates.get_template("workflow_summary.html").render(
        style=style,
        workflows=workflows,
        workflow_statistics=workflow_statistics,
        unique_workflow_types=unique_workflow_types,
    )
    return HTMLResponse(content=content)


async def update_object_name(request: Request, euid, name, _auth=Depends(require_auth)):
    referer = request.headers.get("Referer", "/")
    bobdb = BloomObj(BLOOMdb3(app_username=request.session['user_data']['email']))
    obj = bobdb.get_by_euid(euid)
    if obj:
        obj.name = name  # Update the name
        flag_modified(obj, "name")  # Explicitly mark the object as modified
        bobdb.session.commit()  # Commit the changes to the database
    # Return a RedirectResponse to redirect the user
    return RedirectResponse(url=referer)


@app.get("/equipment_overview", response_class=HTMLResponse)
async def equipment_overview(request: Request, _auth=Depends(require_auth)):
    bobdb = BloomObj(BLOOMdb3(app_username=request.session['user_data']['email']))

    # Fetch equipment instances and templates
    equipment_instances = (
        bobdb.session.query(bobdb.Base.classes.equipment_instance)
        .filter_by(is_deleted=False)
        .all()
    )
    equipment_templates = (
        bobdb.session.query(bobdb.Base.classes.equipment_template)
        .filter_by(is_deleted=False)
        .all()
    )
    user_data = request.session.get('user_data', {})
    style = {"skin_css": user_data.get('style_css', 'static/skins/bloom.css')}

    content = templates.get_template("equipment_overview.html").render(
        style=style,
        equipment_list=equipment_instances,
        template_list=equipment_templates
    )
    return HTMLResponse(content=content)


async def get_print_labs(_request: Request, _auth=Depends(require_auth)):
    # Replace the following line with the actual logic to retrieve your data
    options = [
        {"value": "option1", "text": "Option 1"},
        {"value": "option2", "text": "Option 2"},
        {"value": "option3", "text": "Option 3"},
        # Add more options as needed
    ]
    return options


@app.get("/reagent_overview", response_class=HTMLResponse)
async def reagent_overview(request: Request, _auth=Depends(require_auth)):
    bobdb = BloomObj(BLOOMdb3(app_username=request.session['user_data']['email']))

    # Fetch equipment instances and templates
    reagent_instances = (
        bobdb.session.query(bobdb.Base.classes.content_instance)
        .filter_by(is_deleted=False, btype="reagent")
        .all()
    )
    reagent_templates = (
        bobdb.session.query(bobdb.Base.classes.content_template)
        .filter_by(is_deleted=False, btype="reagent")
        .all()
    )
    user_data = request.session.get('user_data', {})
    style = {"skin_css": user_data.get('style_css', 'static/skins/bloom.css')}

    content = templates.get_template("reagent_overview.html").render(
        style=style,
        instance_list=reagent_instances,
        template_list=reagent_templates,
    )
    return HTMLResponse(content=content)


@app.get("/control_overview", response_class=HTMLResponse)
async def control_overview(request: Request, _auth=Depends(require_auth)):
    bobdb = BloomObj(BLOOMdb3(app_username=request.session['user_data']['email']))

    # Fetch equipment instances and templates
    control_instances = (
        bobdb.session.query(bobdb.Base.classes.content_instance)
        .filter_by(is_deleted=False, btype="control")
        .all()
    )
    control_templates = (
        bobdb.session.query(bobdb.Base.classes.content_template)
        .filter_by(is_deleted=False, btype="control")
        .all()
    )
    user_data = request.session.get('user_data', {})
    style = {"skin_css": user_data.get('style_css', 'static/skins/bloom.css')}

    content = templates.get_template("control_overview.html").render(
        style=style,
        instance_list=control_instances,
        template_list=control_templates
    )
    return HTMLResponse(content=content)


@app.post("/create_from_template")
async def create_from_template(request: Request, euid: str = Form(...), _auth=Depends(require_auth)):
    bobdb = BloomObj(BLOOMdb3(app_username=request.session['user_data']['email']))
    template = bobdb.create_instances(euid)

    if template:
        return RedirectResponse(url=f"/euid_details?euid={template[0][0].euid}", status_code=303)
    else:
        return RedirectResponse(url="/equipment_overview", status_code=303)


@app.get("/uuid_details", response_class=HTMLResponse)
async def uuid_details(request: Request, uuid: str, _auth=Depends(require_auth)):
    bobdb = BloomObj(BLOOMdb3(app_username=request.session['user_data']['email']))
    obj = bobdb.get(uuid)
    return RedirectResponse(url=f"/euid_details?euid={obj.euid}")


@app.get("/vertical_exp", response_class=HTMLResponse)
async def vertical_exp(request: Request, euid=None, _auth=Depends(require_auth)):
    bobdb = BloomObj(BLOOMdb3(app_username=request.session['user_data']['email']))
    instance = bobdb.get_by_euid(euid)
    user_data = request.session.get('user_data', {})
    style = {"skin_css": user_data.get('style_css', 'static/skins/bloom.css')}

    content = templates.get_template("vertical_exp.html").render(
        style=style,
        instance=instance
    )
    return HTMLResponse(content=content)


@app.get("/plate_carosel2", response_class=HTMLResponse)
async def plate_carosel(request: Request, plate_euid: str = Query(...), _auth=Depends(require_auth)):
    bobdb = BloomObj(BLOOMdb3(app_username=request.session['user_data']['email']))

    # Fetch the main plate and its wells
    main_plate = bobdb.get_by_euid(plate_euid)
    oplt = bobdb.get_by_euid("CX42")
    if not main_plate:
        return "Main plate not found."

    # Example logic to fetch related plates (modify based on your data model)
    related_plates = await get_related_plates(main_plate)
    related_plates.append(main_plate)
    # Render the template with the main plate and related plates data
    user_data = request.session.get('user_data', {})
    style = {"skin_css": user_data.get('style_css', 'static/skins/bloom.css')}

    content = templates.get_template("vertical_exp.html").render(
        style=style,
        main_plate=main_plate,
        related_plates=related_plates
    )
    return HTMLResponse(content=content)


async def get_related_plates(request: Request, main_plate, _auth=Depends(require_auth)):
    bobdb = BloomObj(BLOOMdb3(app_username=request.session['user_data']['email']))
    related_plates = []

    # Fetching ancestor plates through parent_of_lineages
    for parent_lineage in main_plate.parent_of_lineages:
        if parent_lineage.child_instance.btype == "plate":
            related_plates.append(parent_lineage.child_instance)

    # Fetching descendant plates through child_of_lineages
    for child_lineage in main_plate.child_of_lineages:
        if child_lineage.parent_instance.btype == "plate":
            related_plates.append(child_lineage.parent_instance)

    # Remove duplicates from the related_plates list
    related_plates = list({plate.euid: plate for plate in related_plates}.values())

    # Additional logic to calculate rows and columns for each related plate
    for plate in related_plates:
        num_rows = 0
        num_cols = 0
        for lineage in plate.parent_of_lineages:
            if (
                    lineage.parent_instance.euid == plate.euid
                    and lineage.child_instance.btype == "well"
            ):
                cd = lineage.child_instance.json_addl.get("cont_address", {})
                num_rows = max(num_rows, int(cd.get("row_idx", 0)))
                num_cols = max(num_cols, int(cd.get("col_idx", 0)))
        plate.json_addl["properties"]["num_rows"] = num_rows + 1
        plate.json_addl["properties"]["num_cols"] = num_cols + 1
        flag_modified(plate, "json_addl")

    return related_plates


@app.get("/plate_visualization")
async def plate_visualization(request: Request, plate_euid, _auth=Depends(require_auth)):
    bobdb = BloomObj(BLOOMdb3(app_username=request.session['user_data']['email']))
    # Fetch the plate and its wells
    plate = bobdb.get_by_euid(plate_euid)

    num_rows = 0
    num_cols = 0

    for i in plate.parent_of_lineages:
        if (
                i.parent_instance.euid == plate.euid
                and i.child_instance.btype == "well"
        ):
            cd = i.child_instance.json_addl["cont_address"]
            if int(cd["row_idx"]) > num_rows:
                num_rows = int(cd["row_idx"])
            if int(cd["col_idx"]) > num_cols:
                num_cols = int(cd["col_idx"])
    plate.json_addl["properties"]["num_rows"] = num_rows + 1
    plate.json_addl["properties"]["num_cols"] = num_cols + 1
    flag_modified(plate, "json_addl")
    bobdb.session.commit()
    if not plate:
        return "Plate not found."
        # Render the template with the plate data
    user_data = request.session.get('user_data', {})
    style = {"skin_css": user_data.get('style_css', 'static/skins/bloom.css')}

    content = templates.get_template("plate_display.html").render(
        style=style,
        plate=plate,
        get_well_color=get_well_color
        )
    return HTMLResponse(content=content)

    # What is the correct path for {style.skin.css}?


@app.get("/database_statistics", response_class=HTMLResponse)
async def database_statistics(request: Request, _auth=Depends(require_auth)):
    bobdb = BloomObj(BLOOMdb3(app_username=request.session['user_data']['email']))

    async def get_stats(days):
        cutoff_date = datetime.now() - timedelta(days=days)
        # Assume bobdb.session.query can be awaited; if not, adjust accordingly
        return (
            await bobdb.session.query(
                bobdb.Base.classes.generic_instance.b_sub_type,
                func.count(bobdb.Base.classes.generic_instance.uuid),
            )
            .filter(
                bobdb.Base.classes.generic_instance.created_dt >= cutoff_date,
                bobdb.Base.classes.generic_instance.is_deleted == False,
            )
            .group_by(bobdb.Base.classes.generic_instance.b_sub_type)
            .all()
        )

    stats_1d = await get_stats(1)
    stats_7d = await get_stats(7)
    stats_30d = await get_stats(30)

    user_data = request.session.get('user_data', {})
    style = {"skin_css": user_data.get('style_css', 'static/skins/bloom.css')}

    content = templates.get_template("database_statistics.html").render(
        request=request,
        stats_1d=stats_1d, stats_7d=stats_7d, stats_30d=stats_30d,
        style=style
    )
    return HTMLResponse(content=content)


@app.get("/object_templates_summary", response_class=HTMLResponse)
async def object_templates_summary(request: Request, _auth=Depends(require_auth)):
    bobdb = BloomObj(BLOOMdb3(app_username=request.session['user_data']['email']))

    # Fetch all generic templates
    generic_templates = (
        bobdb.session.query(bobdb.Base.classes.generic_template)
        .filter_by(is_deleted=False)
        .all()
    )

    # Get unique polymorphic_discriminators
    unique_discriminators = sorted(
        set(t.polymorphic_discriminator for t in generic_templates)
    )
    user_data = request.session.get('user_data', {})
    style = {"skin_css": user_data.get('style_css', 'static/skins/bloom.css')}

    content = templates.get_template("object_templates_summary.html").render(
        request=request,
        generic_templates=generic_templates,
        unique_discriminators=unique_discriminators,
        style=style

    )
    return HTMLResponse(content=content)


@app.get("/euid_details")
async def euid_details(
        request: Request,
        euid: str = Query(..., description="The EUID to fetch details for"),
        _uuid: str = Query(None, description="Optional UUID parameter"),
        _auth=Depends(require_auth)
):
    bobdb = BloomObj(BLOOMdb3(app_username=request.session['user_data']['email']))

    # Fetch the object using euid
    obj = bobdb.get_by_euid(euid)
    relationship_data = get_relationship_data(obj) if obj else {}

    if not obj:
        return HTTPException(status_code=404, detail="Object not found")

    # Convert the SQLAlchemy object to a dictionary, checking for attribute existence
    obj_dict = {
        column.key: getattr(obj, column.key)
        for column in obj.__table__.columns
        if hasattr(obj, column.key)
    }
    obj_dict['parent_template_euid'] = obj.parent_template.euid if hasattr(obj, 'parent_template') else ""
    audit_logs = bobdb.query_audit_log_by_euid(euid)
    user_data = request.session.get('user_data', {})
    style = {"skin_css": user_data.get('style_css', 'static/skins/bloom.css')}

    content = templates.get_template("euid_details.html").render(
        request=request,
        object=obj_dict,
        style=style,
        relationships=relationship_data,
        audit_logs=audit_logs,
        oobj=obj,
    )
    return HTMLResponse(content=content)


@app.get("/un-delete")
async def un_delete_by_uuid(request: Request, uuid: str = None, euid: str = None, _auth=Depends(require_auth)):
    referer = request.headers.get("Referer", "/default_page")

    if uuid is None or euid is None:
        # Consider using an HTTPException for error handling
        return HTMLResponse(
            content=f"Error: uuid or euid not provided && both are required. {uuid} {euid}",
            status_code=400
        )

    bobdb = BloomObj(BLOOMdb3(app_username=request.session['user_data']['email']))
    del_obj = bobdb.get(uuid)
    if not del_obj:
        # Handling the case where the object does not exist
        return HTMLResponse(content="Object not found.", status_code=404)

    del_obj.is_deleted = False
    bobdb.session.commit()

    return RedirectResponse(url=referer)


@app.get("/bloom_schema_report", response_class=HTMLResponse)
async def bloom_schema_report(request: Request, _auth=Depends(require_auth)):
    bobdb = BloomObj(BLOOMdb3(app_username=request.session['user_data']['email']))
    a_stat = bobdb.query_generic_instance_and_lin_stats()
    b_stats = bobdb.query_generic_template_stats()
    reports = [[a_stat[0]], [a_stat[1]], b_stats]
    nrows = 0
    for i in b_stats:
        nrows += int(i['Total_Templates'])
    for ii in a_stat:
        nrows += int(ii['Total_Instances'])

    user_data = request.session.get('user_data', {})
    style = {"skin_css": user_data.get('style_css', 'static/skins/bloom.css')}

    content = templates.get_template("bloom_schema_report.html").render(
        request=request,
        reports=reports,
        nrows=nrows,
        style=style

    )
    return HTMLResponse(content=content)


def delete_by_euid(request: Request, euid, _auth=Depends(require_auth)):
    referer = request.headers.get("Referer", "/")

    bobdb = BloomObj(BLOOMdb3(app_username=request.session['user_data']['email']))
    bobdb.delete(bobdb.get_by_euid(euid))
    bobdb.session.flush()
    bobdb.session.commit()

    return RedirectResponse(url=referer)


@app.get("/delete_object")
async def delete_object(request: Request, _auth=Depends(require_auth)):
    data = await request.json()
    euid = data.get("euid")
    bobdb = BloomObj(BLOOMdb3(app_username=request.session['user_data']['email']))

    bobdb.delete(bobdb.get_by_euid(euid))
    bobdb.session.flush()
    bobdb.session.commit()

    return {
        "status": "success",
        "message": f"Delete object performed for EUID {euid}",
    }


@app.get("/workflow_details", response_class=HTMLResponse)
async def workflow_details(request: Request, workflow_euid, _auth=Depends(require_auth)):
    bwfdb = BloomWorkflow(BLOOMdb3(app_username=request.session['user_data']['email']))
    workflow = bwfdb.get_sorted_euid(workflow_euid)
    accordion_states = dict(request.session)
    user_data = request.session.get('user_data', {})
    style = {"skin_css": user_data.get('style_css', 'static/skins/bloom.css')}

    content = templates.get_template("workflow_details.html").render(
        request=request,
        workflow=workflow,
        accordion_states=accordion_states,
        style=style,
        udat=request.session['user_data'])
    return HTMLResponse(content=content)


from fastapi import Request, Depends, HTTPException


@app.post("/update_accordion_state")
async def update_accordion_state(request: Request, _auth=Depends(require_auth)):
    data = await request.json()
    step_euid = data["step_euid"]
    state = data["state"]  # Assuming 'state' is either 'open' or some other value indicating the accordion's state
    request.session[step_euid] = state
    return {"status": "success"}


@app.post("/workflow_step_action")
async def workflow_step_action(request: Request, _auth=Depends(require_auth)):
    data = await request.json()
    euid = data.get("euid")
    action = data.get("action")
    action_group = data.get("action_group")
    ds = data.get("ds")
    bobdb = BloomWorkflow(BLOOMdb3(app_username=request.session['user_data']['email']))
    bo = bobdb.get_by_euid(euid)

    ds["curr_user"] = request.session.get("user_data", "bloomui-user")
    udat = request.session.get("user_data", {})
    ds['lab'] = udat.get("print_lab", "BLOOM")
    ds['printer_name'] = udat.get("printer_name", "")
    ds['label_zpl_style'] = udat.get("label_style", "")
    ds['alt_a'] = udat.get("alt_a", "")
    ds['alt_b'] = udat.get("alt_b", "")
    ds['alt_c'] = udat.get("alt_c", )
    ds['alt_d'] = udat.get("alt_d", "")
    ds['alt_e'] = udat.get("alt_e", "")

    if bo.__class__.__name__ == "workflow_instance":
        bwfdb = BloomWorkflow(BLOOMdb3(app_username=request.session['user_data']['email']))
        act = bwfdb.do_action(
            euid, action_ds=ds, action=action, action_group=action_group
        )
    else:
        bwfsdb = BloomWorkflowStep(BLOOMdb3(app_username=request.session['user_data']['email']))
        act = bwfsdb.do_action(
            euid, action_ds=ds, action=action, action_group=action_group
        )

    return {"status": "success", "message": f" {action} performed for EUID {euid}"}


def update_obj_json_addl_properties(request: Request, obj_euid, _auth=Depends(require_auth), **properties):
    """Update the json_addl['properties'] field of an object.  Was originally for just wfs...

    Args:
        obj_euid euid(): OBJECT.euid being edited

    Raises:
        Exception: Cherrypy redirect back to referring page
        cherrypy.HTTPRedirect: _description_

    Returns:
        cherrypy.HTTPRedirect: to referrer
    """
    referer = request.headers.get("Referer", "/default_page")

    bobdb = BloomObj(BLOOMdb3(app_username=request.session['user_data']['email']))
    step = bobdb.get_by_euid(obj_euid)

    if step is None:
        print("Step not found")
        return False

    try:
        for key, values in properties.items():
            if key in step.json_addl["properties"]:
                if isinstance(step.json_addl["properties"][key], list):
                    step.json_addl["properties"][key] = (
                        values if isinstance(values, list) else [values]
                    )
                else:
                    step.json_addl["properties"][key] = values
            if key.endswith("[]"):
                key = key[:-2]
                if key in step.json_addl["properties"]:
                    step.json_addl["properties"][key] = (
                        values if isinstance(values, list) else [values]
                    )
                else:
                    step.json_addl["properties"][key] = values

        # Explicitly mark the object as modified if necessary
        flag_modified(step, "json_addl")

        bobdb.session.flush()
        bobdb.session.commit()
        # Optionally, reload the object to confirm changes
        bobdb.session.refresh(step)

    except Exception as e:
        raise Exception("Error updating step properties:", e)

    return RedirectResponse(url=referer)


@app.get("/dindex2", response_class=HTMLResponse)
async def dindex2(request: Request, globalFilterLevel=6, globalZoom=0, globalStartNodeEUID=None,
                  auth=Depends(require_auth)):
    dag_data = generate_dag_json_from_all_objects_v2(request=request, euid=globalStartNodeEUID,
                                                     depth=globalFilterLevel)
    user_data = request.session.get('user_data', {})
    style = {"skin_css": user_data.get('style_css', 'static/skins/bloom.css')}

    content = templates.get_template("dindex2.html").render(
        request=request,
        style=style,
        globalFilterLevel=globalFilterLevel,
        globalZoom=globalZoom,
        globalStartNodeEUID=globalStartNodeEUID,
        dag_data=dag_data
    )
    return HTMLResponse(content=content)


def add_new_node(request: Request, _auth=Depends(require_auth)):
    bobdb = BloomObj(BLOOMdb3(app_username=request.session['user_data']['email']))

    new_ci = bobdb.Base.classes.container_instance(name="newthing")
    bobdb.session.add(new_ci)
    bobdb.session.commit()
    return {"euid": new_ci.euid}


@app.get("/get_node_info")
async def get_node_info(request: Request, euid, _auth=Depends(require_auth)):
    BO = BloomObj(BLOOMdb3(app_username=request.session['user_data']['email']))
    node_dat = BO.get_by_euid(euid)

    if node_dat:
        return {
            "uuid": str(node_dat.uuid),
            "name": node_dat.name,
            "btype": node_dat.btype,
            "euid": node_dat.euid,
            "b_sub_type": node_dat.b_sub_type,
            "status": node_dat.bstatus,
            "json_addl": json.dumps(node_dat.json_addl),
        }
    else:
        return {"error": "Node not found"}


async def generate_dag_json_from_all_objects(request: Request, _output_file="dag.json", _auth=Depends(require_auth)):
    BO = BloomObj(BLOOMdb3(app_username=request.session['user_data']['email']))
    last_schema_edit_dt = BO.get_most_recent_schema_audit_log_entry()
    request.session["user_data"]["dag_fn"] = f"./dags/{str(request.session)}_dag.json"
    output_file = request.session["user_data"]["dag_fn"]
    if (
            "schema_mod_dt" not in request.session
            or request.session["schema_mod_dt"] != last_schema_edit_dt.changed_at
    ):
        print(
            f"Dag WILL BE Regenerated, Schema Has Changed. {output_file} being generated."
        )
    else:
        print(
            f"Dag Not Regenerated, Schema Has Not Changed. {output_file} unchanged."
        )
        return
    request.session["schema_mod_dt"] = last_schema_edit_dt.changed_at

    colors = {
        "container": "#372568",
        "content": "#4560D5",
        "workflow": "#2CB6F0",
        "workflow_step": "#93FE45",
        "equipment": "#7B0403",
        "object_set": "#FE9B2D",
        "actor": "#FEDC45",
        "test_requisition": "#FDDC45",
        "data": "#FCDC45",
        "generic": "pink",
        "action": "#FEDC25",
    }
    sub_colors = {"well": "#70658c"}

    # SQL query to fetch instances
    instance_query = text("""
        SELECT euid, name, super_type, btype, b_sub_type, version
        FROM generic_instance
        WHERE is_deleted = False;
            """)

    # SQL query to fetch lineages
    lineage_query = text("""
    SELECT 
        lineage.euid AS lineage_euid,
        parent.euid AS parent_euid,
        child.euid AS child_euid
    FROM 
        generic_instance_lineage AS lineage
    JOIN 
        generic_instance AS parent ON lineage.parent_instance_uuid = parent.uuid
    JOIN 
        generic_instance AS child ON lineage.child_instance_uuid = child.uuid
    WHERE 
        lineage.is_deleted = False;

            """)

    # Execute queries
    instance_result = BO.session.execute(instance_query).fetchall()
    lineage_result = BO.session.execute(lineage_query).fetchall()

    # Construct nodes and edges
    nodes = []
    edges = []

    for instance in instance_result:
        node = {
            "data": {
                "id": str(instance.euid),
                "type": "instance",
                "euid": str(instance.euid),
                "name": instance.name,
                "btype": instance.btype,
                "super_type": instance.super_type,
                "b_sub_type": instance.super_type + "." + instance.btype + "." + instance.b_sub_type,
                "version": instance.version,
                "color": colors.get(instance.super_type, "pink"),
            },
            "position": {"x": 0, "y": 0},
            "group": "nodes",
        }
        nodes.append(node)

    for lineage in lineage_result:
        edge = {
            "data": {
                "source": str(lineage.parent_euid),
                "target": str(lineage.child_euid),
                "id": str(lineage.lineage_euid),
            },
            "group": "edges",
        }
        edges.append(edge)

    # Construct JSON structure

    dag_json = {
        "elements": {"nodes": nodes, "edges": edges},
        "style": [
            {
                "selector": "node",
                "style": {"background-color": "data(color)", "label": "data(name)"},
            },
            {
                "selector": "edge",
                "style": {
                    "width": "3px",
                    "line-color": "rgb(204,204,204)",
                    "source-arrow-color": "rgb(204,204,204)",
                    "source-arrow-shape": "triangle",
                    "curve-style": "bezier",
                    "control-point-step-size": "40px",
                },
            },
        ],
        "data": {},
        "zoomingEnabled": True,
        "userZoomingEnabled": True,
        "zoom": 1.8745865634962724,
        "minZoom": 1e-50,
        "maxZoom": 1e50,
        "panningEnabled": True,
        "userPanningEnabled": True,
        "pan": {"x": 180.7595665772659, "y": 52.4950387619553},
        "boxSelectionEnabled": True,
        "renderer": {"name": "canvas"},
    }

    # Write to file
    with open(output_file, "w") as f:
        json.dump(dag_json, f, indent=4)

    print(f"All DAG JSON saved to {output_file}")


def generate_dag_json_from_all_objects_v2(request: Request, euid='AY1', depth=6, _auth=Depends(require_auth)):
    # Default values and setup
    if euid in [None, '', 'None']:
        euid = 'AY1'

    BO = BloomObj(BLOOMdb3(app_username=request.session['user_data']['email']))
    last_schema_edit_dt = BO.get_most_recent_schema_audit_log_entry()

    # Simplify file naming and ensure directory exists
    user_email_sanitized = request.session['user_data']['email'].replace('@', '_').replace('.', '_')
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    output_dir = "./dags"
    os.makedirs(output_dir, exist_ok=True)  # Ensure the directory exists
    output_file = os.path.join(output_dir, f"dag_{user_email_sanitized}_{depth}_{timestamp}_dagv2.json")

    # Check if DAG needs to be regenerated
    schema_mod_dt = request.session.get("schema_mod_dt")
    if schema_mod_dt != last_schema_edit_dt.changed_at.isoformat() or not os.path.exists(output_file):
        print(f"Dag WILL BE Regenerated, Schema Has Changed. {output_file} being generated.")
    else:
        print(f"Dag Not Regenerated, Schema Has Not Changed. {output_file} unchanged.")
        return

    request.session["schema_mod_dt"] = last_schema_edit_dt.changed_at.isoformat()
    request.session["user_data"]["dag_fnv2"] = output_file

    colors = {
        "container": "#372568",
        "content": "#4560D5",
        "workflow": "#2CB6F0",
        "workflow_step": "#93FE45",
        "equipment": "#7B0403",
        "object_set": "#FE9B2D",
        "actor": "#FEDC45",
        "test_requisition": "#FDDC45",
        "data": "#FCDC45",
        "generic": "pink",
        "action": "#FEDC25",
    }
    sub_colors = {"well": "#70658c"}

    #instance_result = []
    instance_result = {}
    lineage_result = {}

    for r in BO.fetch_graph_data_by_node_depth(euid, depth):
        if r[0] in [None, '', 'None']:
            pass
        else:

            instance = {'euid': r[0], 'name': r[2], 'btype': r[3], 'super_type': r[4], 'b_sub_type': r[5],
                        'version': r[6]}
            instance_result[r[0]] = instance

            if r[8] in [None, '', 'None']:
                pass
            else:
                lin_edge = {'parent_euid': r[9], 'child_euid': r[10], 'lineage_euid': r[8]}
                lineage_result[r[8]] = lin_edge

    # Construct nodes and edges
    nodes = []
    edges = []

    for instance_k in instance_result:
        instance = instance_result[instance_k]
        node = {
            "data": {
                "id": str(instance['euid']),
                "type": "instance",
                "euid": str(instance['euid']),
                "name": instance['name'],
                "btype": instance['btype'],
                "super_type": instance['super_type'],
                "b_sub_type": instance['super_type'] + "." + instance['btype'] + "." + instance['b_sub_type'],
                "version": instance['version'],
                "color": colors.get(instance['super_type'], "pink"),
            },
            "position": {"x": 0, "y": 0},
            "group": "nodes",
        }
        nodes.append(node)

    for l_i in lineage_result:
        lineage = lineage_result[l_i]

        edge = {
            "data": {
                "source": str(lineage['parent_euid']),
                "target": str(lineage['child_euid']),
                "id": str(lineage['lineage_euid']),
            },
            "group": "edges",
        }
        edges.append(edge)

    # Construct JSON structure

    dag_json = {
        "elements": {"nodes": nodes, "edges": edges},
        "style": [
            {
                "selector": "node",
                "style": {"background-color": "data(color)", "label": "data(name)"},
            },
            {
                "selector": "edge",
                "style": {
                    "width": "3px",
                    "line-color": "rgb(204,204,204)",
                    "source-arrow-color": "rgb(204,204,204)",
                    "source-arrow-shape": "triangle",
                    "curve-style": "bezier",
                    "control-point-step-size": "40px",
                },
            },
        ],
        "data": {},
        "zoomingEnabled": True,
        "userZoomingEnabled": True,
        "zoom": 1.8745865634962724,
        "minZoom": 1e-50,
        "maxZoom": 1e50,
        "panningEnabled": True,
        "userPanningEnabled": True,
        "pan": {"x": 180.7595665772659, "y": 52.4950387619553},
        "boxSelectionEnabled": True,
        "renderer": {"name": "canvas"},
    }

    # Write to file
    with open(output_file, "w") as f:
        json.dump(dag_json, f, indent=4)

    print(f"All DAG JSON saved to {output_file}")


def get_dag(request: Request, _auth=Depends(require_auth)):
    dag_fn = request.session["user_data"]["dag_fn"]
    dag_data = {"elements": {"nodes": [], "edges": []}}
    if os.path.exists(dag_fn):
        with open(dag_fn, "r") as f:
            dag_data = json.load(f)
    return dag_data


@app.get("/get_dagv2")
async def get_dagv2(request: Request, _euid='AY1', _depth=6, _auth=Depends(require_auth)):
    dag_fn = request.session["user_data"]["dag_fnv2"]
    dag_data = {"elements": {"nodes": [], "edges": []}}
    if os.path.exists(dag_fn):
        with open(dag_fn, "r") as f:
            dag_data = json.load(f)
    return dag_data


@app.post("/update_dag")
async def update_dag(request: Request, _auth=Depends(require_auth)):
    input_json = await request.json()  # Corrected call to request.json()
    filename = f"dag_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    with open(filename, "w") as f:
        json.dump(input_json, f)
    return {"status": "success", "filename": filename}


async def add_new_edge(request: Request, _auth=Depends(require_auth)):
    BO = BloomObj(BLOOMdb3(app_username=request.session['user_data']['email']))
    input_data = await request.json()  # Corrected call to request.json()
    parent_euid = input_data["parent_uuid"]
    child_euid = input_data["child_uuid"]
    # Assuming the method returns the new edge object, you might need to adjust this part
    new_edge = BO.create_generic_instance_lineage_by_euids(parent_euid, child_euid)

    return {"euid": str(new_edge.euid)}


@app.post("/delete_node")
async def delete_node(request: Request, _auth=Depends(require_auth)):
    input_data = await request.json()
    node_euid = input_data["euid"]
    BO = BloomObj(BLOOMdb3(app_username=request.session['user_data']['email']))
    BO.delete(euid=node_euid)
    BO.session.flush()
    BO.session.commit()

    return {
        "status": "success",
        "message": "Node and associated lineage records deleted successfully.",
    }


@app.post("/delete_edge")
async def delete_edge(request: Request, _auth=Depends(require_auth)):
    input_data = await request.json()
    edge_euid = input_data["euid"]

    bobdb = BloomObj(BLOOMdb3(app_username=request.session['user_data']['email']))
    bobdb.delete(bobdb.get_by_euid(edge_euid))
    bobdb.session.flush()
    bobdb.session.commit()

    return {"status": "success", "message": "Edge deleted successfully."}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=58080, reload=True)
