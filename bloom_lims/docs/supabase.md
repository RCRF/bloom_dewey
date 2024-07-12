# [Supabase](https://supabase.com/) Authentication w/[Social OAuth Providers](https://supabase.com/docs/guides/auth/social-login#:~:text=Set%20up%20a%20social%20provider%20with%20Supabase%20Auth%23)
Bloom uses [Supabase](https://supabase.com/) for [authentication](https://supabase.com/auth) (_see: [auth docs](https://supabase.com/docs/guides/auth))_. Supabase is an open-source Firebase alternative. It provides a set of tools to build modern apps with features like authentication, real-time subscriptions, and storage. Bloom only leveraging authentication, with google and github both integrated, with [many more supported by supabase](https://supabase.com/docs/guides/auth/social-login#:~:text=Set%20up%20a%20social%20provider%20with%20Supabase%20Auth%23).


## Supabase Setup
To use Supabase, you need to create an account, then set up an organization which you will create a project for bloom specifically. Once you have a project, you will need to get the API URL and the API Key. These will be used in the Bloom configuration file.

### Step By Step
1. Create an account on [Supabase](https://supabase.com/).
   - _note: the free tier is fully functional, with one catch. After 2 weeks of inactivity, supabase will suspend free projects. When a project is suspended, auth will fail. You will need to log in to the supabase dashboard and unsuspend the project to re-enable auth._
2. Create an [organization](https://supabase.com/dashboard/new) for your project.
3. From your organization dashboard, create a new project.
4. You will then be taken to the project dashboard. From the dashboard, you can access `Project URL`, `Project API keys` (both the `'anon' 'public'` and `'service_role'` `'secret'`) & `JWT Settings - JWT Secret`.
5. Create a new file in the repo root dir named `.env` and add the following:
    ```
    SUPABASE_URL=your_project_url
    SUPABASE_KEY=your_project_anon_public_key
    SUPABASE_WHITELIST_DOMAINS=rcrf.org,daylilyinformatics.com,wgrbtb.farm
    BLOOM_DEWEY_S3_BUCKET_PREFIX=a-prefix-for-your-s3-bucket
    ```

    > _note-1: if the `SUPABASE_WHITELIST_DOMAINS` does not exist, or if it is set to '' or to `all`, no whitelist filtering will occur. If a `csv` string of domains, as seen above, is specified, logins are only allowed from those domains._
    
    > _note-2: This .env file is read when the app is hard re-started, not auto restarts._

    > _note-3: The `BLOOM_DEWEY_S3_BUCKET_PREFIX` is used to locate the appropriate buckets for the file manager, [dewey](./dewey.md) to work with. The prefix pattern for all buckets used by dewey is `^([\w-]+)(-)(\d+)$`, ie: `daylily-dewey-0`. Where `$1$2` is the shared prefix for dewey, ie: `daylily-dewey-` buckets and `$3` is an integer which dewey uses to place new files based on if the `euid` in relation to `$3`.  *_this is just a suggestion, no code tries to find files by inferring anything other than the euid encoded in the file name._* This is a simple mechanism to allow rolling to a new S3 bucket when needed. [Learn more in the dewey docs](./dewey.md).


6. Return to your project dashboard. There are lots of other settings you can tweak if you like, but the last thing we need to do is enable an auth provider.  In this example, we will enable Google.
   - Click on the `Authentication` button on the far left side of the dashboard.
   - Click on the `Providers` button on the second tab in from the left.
   - Click on the `Google` button in the long list of providers ( only email should be enabled at this point).
   - Click on the `Enable Sign in with Google` toggle.
   - You will be prompted to enter your Google Client ID and Secret. **At this time, if you have not already set up a google Oauth client, you will need to do so. see below**
     - Change the `Client ID` to your root domain, ie: `daylilyinformatics.com`
     - For the `Client Secret (for OAuth)`, enter the value from `SUPABASE_KEY` stored in your `.env` file. _note: if you get errors saving the key, be sure if you are copyting it, that there are no line breaks due to wrapping or spaces at the end of the key._
   - `Authorized Client IDs (for Android, One Tap, and Chrome extensions)` can be left blank.
   - `Skip nonce checks` can be left unchecked.
   - `Callback URL (for OAuth)` should have the same root as `SUPABASE_URL` with `/auth/v1/callback` appended to the end. If this is the case, then yo do not need to do anything further.
   - Click the `Save` button.
   - `Google` should now be enabled. You can enable other auth providers in the same way, such as Github, Gitlab, etc. However, each will need a small bit of code written in the jinja2 tempates `index.html` and `login.html` to enable other login buttons.
     - You might need to go back to google cloud to enter the callback URI from this now active service.
     - You might get a warning `OTP expiry exceeds recommended threshold`, which can be ignored for the time being.
   - Click on the `Home` icon in the upper left, which should bring you to the project dashboard. In the upper right of this dashboard will be a status button, which will be yellow if your project is still deploying. Once it is green, you can proceed to the next step. _note: this will appear yellow if the project has been suspended for inactivity.  You will have access to a button to unsuspend the project from this view._
7. For more extensive config of supabase (ie: only allowing access from certain domains, etc, [please see their docs](https://supabase.com/docs)).
8. You can now run the bloom UI and test the auth functionality.

## URL Configuration
If you run bloom on the machine your web browser is running, then there is nothing more for you to do. However, if you are running bloom on a remote server, the `url configuration` must be changed to match the IP specifically used to run bloom remotely. You can not use self shorcuts like `localhost` or `0.0.0.0`, etc.
So, change this `http://127.0.0.1:3000` to `http://your.ip.address:3000` in the `URL_Configuration` found in the supabase project Authentication settings.

# NOTE ... HTTPS NEEDS TO BE ENABLED ^^^^^ I JUST NOTICED THIS IS USING HTTP :-/

# Google OAuth Setup

To set up Google OAuth with Supabase, you need to obtain the OAuth 2.0 client ID and client secret from Google Cloud. Here's a step-by-step guide on how to do this:

1.  Create a Google Cloud Project
      Go to the Google Cloud Console.
      Click on the project dropdown and select New Project.
      Enter a project name and click Create.
2.  Configure OAuth Consent Screen
      In the Google Cloud Console, navigate to APIs & Services > OAuth consent screen.
      Select your user type (e.g., External) and click Create.
      Fill in the required fields such as App name, User support email, and Developer contact information.
      Click Save and Continue.
3.  Create OAuth 2.0 Credentials
      In the Google Cloud Console, navigate to APIs & Services > Credentials.
      Click on Create Credentials and select OAuth 2.0 Client ID.
      Select the application type (e.g., Web application).
      Fill in the required fields, such as the name and authorized redirect URIs. The redirect URIs should include:
      https://<your-supabase-project-id>.supabase.co/auth/v1/callback
      Click Create.
4.  Obtain Client ID and Client Secret
      After creating the OAuth 2.0 client ID, a dialog will appear showing the client ID and client secret.
      Copy the client ID and client secret to a safe place. You will need these to configure Supabase.
