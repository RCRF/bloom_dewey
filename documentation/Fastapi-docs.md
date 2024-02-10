the purpose of this document is to explain the need for fastapi compared to cherrypy 
and what was changed when migrating to fastapi

Why the need?

- the primary need is to allow for async operations to allow for many simultaneous transactions
- type hints for data validation comes built in
- Generates automatic api documentation through swagger making future automated tests easier.

What changed?

- The functions themselves did not change in functionality, apart from majority becoming async.
- Since fastapi can handle request validation out the box the authentication needed to be adjusted
accordingly. 
- Updated the global auth sessions to pass the data along through fastapi sessions
as well as storing the user email in the udat.json file
- The main ui file is no longer bloomuiiu.py it is main.py in accordance with fastapi
- fastapi also requires a local server to be started in order to start the application. the command to start the application is
  **uvicorn main:app --host 0.0.0.0 --port 58080 --reload**



Currently, there are most likely some unexpected bugs. Testing will need to be taken place to ensure all is working
as it was with cherrypy.


Some updates needed to be made

- when a user logs out ensure the session ends
- update the style skins to allow users to update such, currently i have forced the
bloom.css skin throughout all pages.

