import json
import math
import dateutil.parser
import datetime
import time
import os
import logging
import boto3
import re
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# DynamoDB resource and SES client
dynamodb_resource = boto3.resource('dynamodb')
ses_client = boto3.client('ses')

# SQS Queue URL
SQS_URL = "https://sqs.us-west-2.amazonaws.com/476114143007/DiningSuggestionsQueue"

# SQS sender function
def sendSQS(request_data):
    sqs_client = boto3.client('sqs')
    response = sqs_client.send_message(
        QueueUrl=SQS_URL,  
        MessageAttributes={
            "location": {'DataType': 'String', 'StringValue': request_data["Location"]},
            "Cuisine": {'DataType': 'String', 'StringValue': request_data["Cuisine"]},
            "NumberOfPeople": {'DataType': 'Number', 'StringValue': request_data["NumberOfPeople"]},
            "DiningTime": {'DataType': 'String', 'StringValue': request_data["DiningTime"]},
            "email": {'DataType': 'String', 'StringValue': request_data["email"]}
        },
        MessageBody='Restaurant slots'
    )
    return response 

""" --- Helpers for responses and slot management --- """
def get_slots(intent_request):
    return intent_request['currentIntent']['slots']

def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }

def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }
    return response

def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }

""" --- Helper Functions --- """
def parse_int(n):
    try:
        return int(n)
    except ValueError:
        return float('nan')

def build_validation_result(is_valid, violated_slot, message_content):
    if message_content is None:
        return {"isValid": is_valid, "violatedSlot": violated_slot}
    return {
        'isValid': is_valid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }

def valid_email(email):
    regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    return re.fullmatch(regex, email) is not None

""" --- Validation Function for Dining Suggestions --- """
def validate_dining_suggestions(location, cuisine, number_of_people, dining_time, email):
    valid_cuisines = ['chinese', 'indian', 'italian', 'japanese', 'korean', 'mex']

    # Validate Location (Example: Only Manhattan for this scenario)
    if location and location.lower() != "manhattan":
        return build_validation_result(False, 'Location', "Currently, we only support restaurant suggestions in Manhattan.")

    # Validate Cuisine
    if cuisine and cuisine.lower() not in valid_cuisines:
        return build_validation_result(False, 'Cuisine', f"We do not support {cuisine} cuisine yet. How about trying Italian instead?")

    # Validate Number of People
    if number_of_people:
        number_of_people = parse_int(number_of_people)
        if not 0 < number_of_people < 30:
            return build_validation_result(False, 'NumberOfPeople', "Please provide a number of people between 1 and 30.")

    # Validate Dining Time (optional, you can adjust this as needed)
    if dining_time:
        try:
            time.strptime(dining_time, '%H:%M')
        except ValueError:
            return build_validation_result(False, 'DiningTime', "Please provide a valid time in the format HH:MM.")

    # Validate Email
    if email and not valid_email(email):
        return build_validation_result(False, 'email', "Please provide a valid email address.")

    return build_validation_result(True, None, None)

""" --- Intent Handlers --- """
def dining_suggestions(intent_request):
    slots = get_slots(intent_request)
    location = slots["Location"]
    cuisine = slots["Cuisine"]
    number_of_people = slots["NumberOfPeople"]
    dining_time = slots["DiningTime"]
    email = slots["email"]
    source = intent_request['invocationSource']
    session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] else {}

    # Validation
    if source == 'DialogCodeHook':
        validation_result = validate_dining_suggestions(location, cuisine, number_of_people, dining_time, email)
        if not validation_result['isValid']:
            slots[validation_result['violatedSlot']] = None
            return elicit_slot(session_attributes, intent_request['currentIntent']['name'], slots, validation_result['violatedSlot'], validation_result['message'])

        # Delegate the rest of the slots to Lex if validation passes
        return delegate(session_attributes, slots)

    # Prepare SQS message data
    request_data = {
        "Location": location,
        "Cuisine": cuisine,
        "NumberOfPeople": number_of_people,
        "DiningTime": dining_time,
        "email": email
    }

    # Send data to SQS
    sendSQS(request_data)

    return close(session_attributes, 'Fulfilled', {
        'contentType': 'PlainText',
        'content': "You’re all set. Expect my suggestions shortly!"
    })

def greeting_intent(intent_request):
    return close(intent_request['sessionAttributes'], 'Fulfilled', {
        'contentType': 'PlainText', 'content': 'Hello! How can I assist you today?'
    })

def thank_you_intent(intent_request):
    return close(intent_request['sessionAttributes'], 'Fulfilled', {
        'contentType': 'PlainText', 'content': "You're welcome!"
    })

""" --- Main Handler --- """
def dispatch(intent_request):
    intent_name = intent_request['currentIntent']['name']

    if intent_name == 'DiningSuggestionsIntent':
        return dining_suggestions(intent_request)
    elif intent_name == 'GreetingIntent':
        return greeting_intent(intent_request)
    elif intent_name == 'ThankYouIntent':
        return thank_you_intent(intent_request)

    raise Exception(f"Intent with name {intent_name} not supported.")

def lambda_handler(event, context):
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    return dispatch(event)
