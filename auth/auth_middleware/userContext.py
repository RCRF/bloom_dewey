from auth.supabase.connection import create_supabase_client

class UserContext:
    def __init__(self):
        
        from IPython import embed
        embed() 
        self.supabase = create_supabase_client()

    def get_current_session(self):
        """Retrieve the current session."""
        return self.supabase.auth.get_session()

    def get_current_user(self):
        """Retrieve the currently logged-in user."""
        return self.supabase.auth.get_user()

    def create_new_user(self, email, password):
        """Create a new user with the given email and password."""
        credentials = {'email': email, 'password': password}
        return self.supabase.auth.sign_up(credentials)

# Example usage
user_context = UserContext()

# To get the current session
current_session = user_context.get_current_session()
print(current_session)

# To get the current user
current_user = user_context.get_current_user()
print(current_user)

# Create a new user
email = "joshswebdevelopment@gmail.com"
password = "notapplicable"
response = user_context.create_new_user(email, password)
print(response)  # This will print the response from the Supabase API

