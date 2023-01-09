This project is meant to expose a comfortable UI for the database.

Currently the following API is supported by the backend:

A websocket is opened in IP localhost:5678/ws

Only two requests are supported:

1. DB creation request.
View the function "CreateTable" in sample_client.html

2. Query request.
View the function "SendQuery" in sample_client.html

The backend response to both of these requests with a response that look like this:

{
    "Success" : <bool>
	"Error" :  <string>
	"Data"    <string[][]> - an array of string arrays
}

Success - indicates whether or not the operation succeeded.
Error - explains what went wrong in case Success==false.
Data - optional, contains the record retrieved via a "select" query.