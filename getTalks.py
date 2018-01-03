import boto3
import json
import logging
import os
import elasticsearch
from requests_aws4auth import AWS4Auth
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
import time

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('RateMyTalkSessions')
es_host = 'search-myawstalks-6cipx2o3dnhiqah2as4a2drfci.us-east-1.es.amazonaws.com'

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
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }

def build_response_card(title, subtitle, options):
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
    options = []

    for i in range(min(len(sessions), 5)):
        options.append({'text': sessions[i], 'value': sessions[i]})

    return options

def get_session(session_date):
    try:
        response = table.scan(
            FilterExpression=Attr('session_date').between(session_date, datetime.now().strftime("%Y-%m-%d"))
        )
        items = response[u'Items']

        buttons = []

        if items:
            for item in items:
                buttons.append(item['session_name'])

        return buttons

    except ClientError as e:
        logger.error(e.response['Error']['Message'])
        return None

def get_full_session(session_name, session_date):
    try:
        response = table.query(
            KeyConditionExpression=Key('session_date').eq(session_date) & Key('session_name').eq(session_name)
        )
        items = response[u'Items']
        logger.debug(items)

        if items:
            return items
        else:
            logger.info('No session details found for ES submission')
            return items

    except ClientError as e:
        logger.error(e.response['Error']['Message'])
        return None

def insert_into_es(record_id, record):
    try:
        cred = boto3.session.Session().get_credentials()
        awsauth = AWS4Auth(cred.access_key,
                           cred.secret_key,
                           os.environ.get('AWS_DEFAULT_REGION'),
                           'es',
                           session_token=cred.token)
        es = elasticsearch.Elasticsearch(
            hosts=[es_host],
            connection_class=elasticsearch.RequestsHttpConnection,
            http_auth=awsauth,
            use_ssl=True,
            verify_certs=True,
            port=443)
        es.info()
    except Exception as e:
        print("Failed to connect to Amazon ES, because %s" % (e))
        raise(e)
    try:
        myindex = datetime.datetime.now().strftime("talks-review-%Y-%m")
        es.index(index=myindex, doc_type='record', id=record_id, body=record)
        logger.info('Wrote record: ' + record)
    except Exception as e:
        print("Failed to insert record to Amazon ES, because %s" % (e))
        raise(e)

def save_data(session_attributes, session_score, record_id):
    print('Saving Data')
    print ('Session Data: ' + session_attributes)
    print ('Session Score: ' + session_score)
    print ('Record ID: ' + record_id)
    
    for record in session_attributes:
        try:
            insert_into_es(record_id, record)
        except Exception as e:
            print("Failed to insert into ES. %s" % (e))
            print(json.dumps(record))

def get_my_talks(event, context):
    logger.info('Received event: ' + json.dumps(event))

    intent = event['currentIntent']['name']
    session_name = event['currentIntent']['slots']['sessionName']
    session_date = event['currentIntent']['slots']['sessionDate']
    session_score = event['currentIntent']['slots']['sessionScore']
    record_id = context.aws_request_id

    if session_date and not session_name:
        mySession = get_session(session_date)
        logger.info(mySession)

        if mySession:
            return elicit_slot(None, intent, event['currentIntent']['slots'], 'sessionName',
            {'contentType': 'PlainText', 'content': 'Please select a session from the next cards.'},
            build_response_card('Availible Sessions', 'Please select a session you would like to rate.', build_options(mySession))
            )

        else:
            last_month = datetime.now() - timedelta(days=30)
            last_month = last_month.strftime("%Y-%m-%d")

            mySession = get_session(last_month)
            return elicit_slot(None, intent, event['currentIntent']['slots'], 'sessionName',
            {'contentType': 'PlainText', 'content': 'There are no sesions in this timeframe. Here are all the sessions from the last month'},
            build_response_card('Availible Sessions', 'Please select a session you would like to rate.', build_options(mySession))
            )

    if session_date and session_name and session_score:
        if event['currentIntent']['confirmationStatus']=='None':
            return confirm_intent(None, intent, event['currentIntent']['slots'],
            {'contentType': 'PlainText', 'content': 'Are you OK with sending the score %s for the session %s on %s?' % (session_score, session_name, session_date)}, None)
        else:
            save_data(get_full_session(session_name, session_date), session_score, record_id)
            return close(None, 'Fulfilled',
            {'contentType': 'PlainText', 'content': 'Thank you for rating the session!'})
    else:
        logger.info('Responding with: dialogAction type Delegate')
        return delegate(None, event['currentIntent']['slots'])
