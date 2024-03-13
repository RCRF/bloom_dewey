Supabase Configuration Guide

Below is a list of steps I took in order to establish a working supabase connection and providers for authentication.

Please refer to official documentation at (https://supabase.com/docs/guides/auth) for additional assistance.
If you have issues please create an issue in the bloom repository and I will be available to assist.


1. Setting up initial supabase connection.
   - Create a supabase account and project.
   - Obtain your SUPABASE_URL and SUPABASE_KEY.
   - I am using dotenv python package to securely store and pull these env files.
   - Once you have your .env variables, store them in a .env file. Put them in this format (SUPABASE_URL="https://your_superbase_project.supabase.co")(SUPABASE_KEY="Secret Key")
   - From there you have a proper connection to supabase. The location of the file is (auth.supabase.connection.py)


This step will show you how to set up providers, keep in mind all providers configurations are different and require different setups.
In this project we are using GitHub and Google.

2. Setting up providers (GitHub)
   - In Supabase go to the authentication page and click on providers.
   - Start by enabling the GitHub provider.
   - Copy the Callback Url it should look like: https://project_id.supabase.co/auth/v1/callback
   - Go to your GitHub account -> Click on your profile picture -> Settings -> Developer Settings -> OAuth Apps
   - Create a new OAuth app
   - Enter the application name, homepage url (http://localhost, or your domain name), and paste the Callback URL from step 2.3.
   - Register your app.
   - Copy the client id and secret key and paste it into the supabase GitHub provider fields.
   - Save the provider and now your GitHub provider is set up for your application.

3. Setting up providers (Google)
   - In Supabase go to the authentication page and click on providers.
   - Start by enabling the Google provider.
   - Copy the Callback Url it should look like: https://project_id.supabase.co/auth/v1/callback
   - If you have not already created a Google platform account and in the search bar navigate to the OAuth Consent Screen
   - In this view click External to allow certain users to test the auth
   - On the side panel above the OAuth consent screen click on "Credentials"
   - Once loaded, click on "Create Credentials"
   - In the Authorised redirect URLs paste your callback URL
   - Click create and copy the Client ID and Client Secret
   - Paste these values into the Supabase provider dropdown
   - **VERY IMPORTANT: When deploying to a live environment ensure you change the project in google**
   - Now your Google provider is set up.

4. Change the project id variable in the index.html page
   - Copy your project id and replace it with your_project_id in the login.html file: **window.location.href = 'https://your_project_id.supabase.co/auth/v1/authorize?provider=google&userinfo.email'**
  ** I think this is outdated, and the edits only need to happen in the login.html file? **


**Additional Info**

Where supabase is being used for the authentication process includes:

- bloomuiiu.py: methods affected - oauth_callback & login
- index.html
- login.html

Please keep in mind this is a quick setup and does not include POST or GET requests to your database.
Also, this does not currently handle errors gracefully. This code currently provides a basic login/signup structure using Supabase. 
Future additions will be made to ensure proper error handling and database management.

- Also, this project does not handle custom passwords, there is a default password for all users signing up using a traditional email login. Located at login.html
- If a user signs up with a provider and then signs in later with just their email, an error will be triggered.

**Helpful Links**

- https://supabase.com/docs/guides/auth/social-login/auth-google
- https://supabase.com/docs/guides/auth/social-login/auth-github
- https://supabase.com/docs/guides/auth/auth-email
   