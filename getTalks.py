import boto3
import json
import logging
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

from datetime import datetime, timedelta
import time

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('RateMyTalkSessions')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message, response_card):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message,
            'responseCard': response_card
        }
    }

def confirm_intent(session_attributes, intent_name, slots, message, response_card):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ConfirmIntent',
            'intentName': intent_name,
            'slots': slots,
            'message': message,
            'responseCard': response_card
        }
    }

def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
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

def build_response_card(title, subtitle, options):
    """
    Build a responseCard with a title, subtitle, and an optional set of options which should be displayed as buttons.
    """
    buttons = None
    if options is not None:
        buttons = []
        for i in range(min(5, len(options))):
            buttons.append(options[i])

    return {
        'contentType': 'application/vnd.amazonaws.card.generic',
        'version': 1,
        'genericAttachments': [{
            'title': title,
            'subTitle': subtitle,
            'buttons': buttons
        }]
    }

def build_options(sessions):
    """
    Build a list of potential sessions for rating, to be used in responseCard generation.
    """
    options = []

    for i in range(min(len(sessions), 5)):
        options.append({'text': sessions[i], 'value': sessions[i]})

    return options

def getSession(session_date):
    try:
        response = table.scan(
            FilterExpression=Attr('session_date').gte(session_date)
        )
        items = response[u'Items']

        if items:
            buttons = []
            for item in items:
                buttons.append(item['session_name'])

        return buttons
    
    except ClientError as e:
        logger.error(e.response['Error']['Message'])
        return None

def getMyTalks(event, context):
    #return event
    logger.info('Received event: ' + json.dumps(event))

    intent = event['currentIntent']['name']
    session_name = event['currentIntent']['slots']['sessionName']
    session_date = event['currentIntent']['slots']['sessionDate']
    session_score = event['currentIntent']['slots']['sessionScore']

    if intent == 'RateTalk':
        lookup_val = datetime.now() - timedelta(days=30)
        lookup_val = lookup_val.strftime("%Y-%m-%d")

    if session_date:
        mySession = getSession(session_date)
        logger.info(mySession)

        if mySession:
            return elicit_slot(None, intent, event['currentIntent']['slots'], 'sessionName',
            {'contentType': 'PlainText', 'content': 'Please select a session from the next cards.'},
            build_response_card('Availible Sessions', 'Please select the session you would like to rate.', build_options(mySession))
            )

        else:
            return elicit_slot(None, intent, event['currentIntent']['slots'], 'sessionDate',
                {'contentType': 'PlainText', 'content': 'There are no sessions in this timeframe. Please specify a session date from the last month.'} )


    if session_date and session_name and session_score>0:
        if item:
            session_time = datetime.fromtimestamp(session_time).strftime('%B %d at %H:%M')
            content = 'Next session%s in the AWS Tel Aviv Loft is:  %s at %s' % (add_tomorrow, item['session_name'], session_time)
        else:
            content = 'I could not find anymore sessions for today. You can ask me to lookup for the next session tomorrow.'

        logger.info('Responding with: ' + content)

    else:
        logger.info('Responding with: dialogAction type Delegate')
        return delegate(None, event['currentIntent']['slots'])
