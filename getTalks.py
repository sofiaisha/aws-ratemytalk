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

    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('RateMyTalkSessions')

    intent = event['currentIntent']['name']
    session_name = event['currentIntent']['slots']['sessionName']
    session_date = event['currentIntent']['slots']['sessionDate']

    if intent == 'RateTalk':
        lookup_val = datetime.now() - timedelta(days=30)
        lookup_val = lookup_val.strftime("%Y-%m-%d")

    if session_date:
        mySession = getSession(session_date)

    if session_date and session_name and sessionScore>0:
        if item:
            session_time = datetime.fromtimestamp(session_time).strftime('%B %d at %H:%M')
            content = 'Next session%s in the AWS Tel Aviv Loft is:  %s at %s' % (add_tomorrow, item['session_name'], session_time)
        else:
            content = 'I could not find anymore sessions for today. You can ask me to lookup for the next session tomorrow.'

        logger.info('Responding with: ' + content)
        return {
              'version': '1.0',
              'sessionAttributes': {},
              'response': {
                'outputSpeech': {
                  'type': 'PlainText',
                  'text': content
                },
                'card': {
                  'type': 'Simple',
                  'title': 'AWS Pop-up Loft Tel Aviv Sessions',
                  'content': content
                },
                'shouldEndSession': False
              }
        }

def getSession(session_date):
    try:
        response = table.scan(
            FilterExpression=Attr('session_date').gte(lookup_val)
        )
        items = response['Items']
        print items


    except ClientError as e:
        logger.error(e.response['Error']['Message'])
        raise SystemExit
    else:
        item = response[u'Items']

        for item in response['Items']:
            session_time = item['timestamp']
            speaker = item['speaker']
            session_name = item['session_name']

        logger.info(item)

        return item
