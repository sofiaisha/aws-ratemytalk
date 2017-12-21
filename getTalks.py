import boto3
import json
import logging
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

from datetime import datetime, timedelta
import time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

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
        logger.info('My Sessions: ' + json.dumps(mySession))

        if mySession == 'null':
            return {
                "dialogAction": {
                    "type": "ElicitSlot",
                    "message": {
                        "contentType": "PlainText",
                        "content": "There are no sessions in this timeframe. Please specify a session date from the last month."
                    },
                    "intentName": "RateTalk",
                    "slots": {
                        "sessionName": session_name,
                        "sessionDate": 'null',
                        "sessionScore": session_score
                    },
                    "slotToElicit" : "sessionDate"
                }
            }

        else:
            return {
                "sessionAttributes": {},
                "dialogAction": {
                    "type": "Delegate",
                    "slots": {
                        "sessionName": session_name,
                        "sessionDate": session_date,
                        "sessionScore": session_score
                    }
                },
                "responseCard": {
                    "version": 1,
                    "contentType": "application/vnd.amazonaws.card.generic",
                    "genericAttachments": [
                    {
                        "title": "Availible Sessions",
                        "subtitle": "Please select the session you would like to rate.",
                        "buttons": [
                        "%s"
                        ]
                    }
                    ]
                }
            } % (mySession)

    if session_date and session_name and session_score>0:
        if item:
            session_time = datetime.fromtimestamp(session_time).strftime('%B %d at %H:%M')
            content = 'Next session%s in the AWS Tel Aviv Loft is:  %s at %s' % (add_tomorrow, item['session_name'], session_time)
        else:
            content = 'I could not find anymore sessions for today. You can ask me to lookup for the next session tomorrow.'

        logger.info('Responding with: ' + content)

    else:
        logger.info('Responding with: dialogAction type Delegate')
        return {
            'sessionAttributes': {},
            'dialogAction': {
                'type': 'Delegate',
                'slots': {
                    'sessionName': session_name,
                    'sessionDate': session_date,
                    'sessionScore': session_score
                }
            }
        }

def getSession(session_date):
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('RateMyTalkSessions')

    try:
        response = table.scan(
            FilterExpression=Attr('session_date').gte(session_date)
        )
        items = response['Items']


    except ClientError as e:
        logger.error(e.response['Error']['Message'])
        raise SystemExit
    else:
        if items:
            item = response[u'Items']
            buttons = ''

            for item in response['Items']:
                if buttons!='':
                    buttons += ","

                session_date = item['session_date']
                session_name = item['session_name']
                buttons += '{"text": "%s","value": "%s"}' % (session_name, session_name)

            logger.info(buttons)
            return buttons
        else:
            return 'null'
