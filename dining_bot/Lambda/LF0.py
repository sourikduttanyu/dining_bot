import json
import boto3
import traceback

# Initialize Lex client
lex_client = boto3.client('lex-runtime')

def process_message(message):
    """
    This function sends the user's message to Amazon Lex and returns the response.
    """
    try:
        # Send the message to Lex
        lex_response = lex_client.post_text(
            botName='DiningBot',  # Your Lex bot's name
            botAlias='DiningBotAlias',  # Your Lex bot's alias
            userId='User123',  # Unique user ID or session ID (can be replaced with a dynamic user ID)
            inputText=message
        )
        
        # Extract the Lex response message
        lex_message = lex_response.get("message", "Sorry, I didn't understand that.")
        
        # Return the message in the expected format
        return {
            "type": "unstructured",
            "unstructured": {
                "id": "1",  # You can use a unique ID if needed
                "text": lex_message,  # The response from Lex
                "timestamp": "2024-10-16T00:00:00Z"  # Replace with actual timestamp if required
            }
        }

    except Exception as e:
        # Log the error if something goes wrong with Lex communication
        print(f"Error in process_message: {str(e)}")
        return {
            "type": "unstructured",
            "unstructured": {
                "id": "1",
                "text": "Sorry, I had trouble understanding that.",
                "timestamp": "2024-10-16T00:00:00Z"
            }
        }


def lambda_handler(event, context):
    """
    The main Lambda handler that processes incoming requests from the API Gateway.
    """
    try:
        # Extract the body from the API Gateway event
        body = event.get("body")
        
        # If body is a string, parse it as JSON
        if isinstance(body, str):
            body = json.loads(body)
        
        # Extract the user's message from the request
        user_message = body["messages"][0]["unstructured"]["text"]
        print(f"User message: {user_message}")  # For debugging
        
        # Process the user's message using Lex
        response_messages = [process_message(user_message)]
        
        # Construct the successful response
        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*", 
                "Access-Control-Allow-Methods": "POST, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type"
            },
            "body": json.dumps({"messages": response_messages})
        }

    except Exception as e:
        # Log the full stack trace if something goes wrong
        print(f"Error: {str(traceback.format_exc())}")
        
        # Construct the error response
        return {
            "statusCode": 500,
            "headers": {
                "Access-Control-Allow-Origin": "*", 
                "Access-Control-Allow-Methods": "POST, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type"
            },
            "body": json.dumps({"code": 500, "message": str(e)})
        }
