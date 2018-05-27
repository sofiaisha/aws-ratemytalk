import boto3
import json
import logging
import os
import elasticsearch
import decimal
from requests_aws4auth import AWS4Auth
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from datetime import datetime, timedelta, time

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('RateMyTalkSessions')
es_host = 'search-myawstalks-6cipx2o3dnhiqah2as4a2drfci.us-east-1.es.amazonaws.com'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

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
        for i in range(min(3, len(options))):
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

def build_options(sessions, start_from = 0):
    options = []
    for i in range(start_from, (min(len(sessions), start_from + 2))):
        options.append({'text': sessions[i]['topic'], 'value': sessions[i]['session_id']})
    if len(options)>1 and i+1 < len(sessions):
        options.append({'text': '*More Sessions*', 'value': 'more'})
    else:
        options.append({'text': '*Start Over*', 'value': 'start_over'})

    return options

def get_session(session_date):
    filename = 'sessions_list'
    cache = read_cache(filename)
    if not cache:
        try:
            response = table.query(
                IndexName='public-date-index',
                #KeyConditionExpression=Key('public').eq(1) & Key('date').lte(datetime.now().strftime("%Y-%m-%d")),
                KeyConditionExpression=Key('public').eq(1),
                ScanIndexForward=False
            )
            items = response[u'Items']

            buttons = []

            if items:
                for item in items:
                    buttons.append(item)

            store_cache(filename, buttons)
            return buttons

        except ClientError as e:
            logger.error(e.response['Error']['Message'])
            return None
    else:
        return cache

def get_session_details(session_id):
    try:
        response = table.get_item(
            Key={
                'session_id': session_id
            }
        )
        if response['Item']:
            items = response['Item']
        if items:
            return items
        else:
            logger.info('No session details found for id %s' % session_id)
            return items

    except ClientError as e:
        logger.error(e.response['Error']['Message'])
        return None

def get_full_session(session_id, session_score, event):
    user_id = event['userId']
    if event['requestAttributes']:
        channel = event['requestAttributes']['x-amz-lex:channel-name']
    else:
        channel = 'External'
    try:
        response = table.get_item(
            Key={
                'session_id': session_id
            }
        )

        items = response['Item']

        if items:
            additional_data = {"session_score":int(session_score),
            "user_id": user_id,
            "channel": channel,
            "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S%z")}
            items.update(additional_data)

            items = json.dumps(items, cls=DecimalEncoder)
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
    print type(record_id)
    try:
        myindex = datetime.now().strftime("talks-review-%Y-%m")
        es.index(index=myindex, doc_type='documents', id=record_id, body=record)
        logger.info('Wrote record: ' + record)
    except Exception as e:
        print("Failed to insert record to Amazon ES, because %s" % (e))
        raise(e)

def save_data(record, record_id):
    try:
        insert_into_es(record_id, record)

    except Exception as e:
        print("Failed to insert into ES. %s" % (e))
        print(json.dumps(record))

def store_cache(filename, data):
    file = open('/tmp/%s' % filename,'w')
    json.dump(data, file, cls=DecimalEncoder)
    file.close()

    logger.info('Wrote cache to %s: %s' % (filename, data))
    return True

def read_cache(filename):
    return False

    try:
        file = open('/tmp/%s' % filename,'r')
        print file
        data = file.read()
        #TODO: Convert the file to the right format (list?)
        file.close()

        logger.info('Got cache from %s: %s' % (filename, data))
        return data

    except Exception as e:
        logger.warning("Failed to read cache file %s. %s" % (filename, e))


def get_my_talks(event, context):
    logger.info('Received event: ' + json.dumps(event))

    intent = event['currentIntent']['name']
    if event['sessionAttributes']:
        start_from = int(event['sessionAttributes']['start_from'])
    else:
        start_from = 0

    session_name = event['currentIntent']['slots']['sessionName']
    session_date = event['currentIntent']['slots']['sessionDate']
    session_score = event['currentIntent']['slots']['sessionScore']
    session_id = event['currentIntent']['slots']['sessionID']
    session_text = event['inputTranscript']

    com_client = boto3.client('comprehend')
    com_response = com_client.detect_entities(Text=session_text, LanguageCode='en')
    logger.info('Comprehand: ' + json.dumps(com_response))

    com_response = com_client.detect_key_phrases(Text=session_text, LanguageCode='en')
    logger.info('Comprehand: ' + json.dumps(com_response))
    
    if session_id == 'start_over':
        start_from = 0
        session_id = None
    if session_id == 'more':
        session_id = None

    record_id = context.aws_request_id

    #start with if not session_id
    if not session_id:
        last_month = datetime.now() - timedelta(days=30)
        last_month = last_month.strftime("%Y-%m-%d")
        my_session = get_session(last_month)

        if my_session:
            next_start = start_from + 2
            if len (my_session) < next_start:
                next_start = 0
            logger.info('Responding with: dialogAction type Elicit Slot sessionID')
            return elicit_slot({'start_from': next_start}, intent, event['currentIntent']['slots'], 'sessionID',
            None,
            build_response_card('Availible Sessions', 'Please select a session you would like to rate.', build_options(my_session, start_from))
            )
        else:
            logger.info('Responding with: dialogAction type Elicit Slot sessionID')
            return elicit_slot(None, intent, event['currentIntent']['slots'], 'sessionID',
            {'contentType': 'PlainText', 'content': 'There are no public sesions from the last month. If you have a specific session ID, please provide it now'},
            None)

    else:
        if session_score:
            if int(float(session_score))>5 or int(float(session_score))<1:
                logger.info('Responding with: dialogAction type Elicit Slot sessionScore')
                return elicit_slot(None, intent, event['currentIntent']['slots'], 'sessionScore',
                {'contentType': 'PlainText', 'content': 'Your score must be *between 1 and 5*'},
                None)
            if event['currentIntent']['confirmationStatus']=='None':
                logger.info('Responding with: dialogAction type Confirm')
                return confirm_intent(None, intent, event['currentIntent']['slots'],
                {'contentType': 'PlainText', 'content': 'Are you OK with sending the score %s for the session %s on %s?' %
                (session_score, session_name, datetime.strptime(session_date,'%Y-%m-%d').strftime("%B %d, %Y"))}, None)
            elif event['currentIntent']['confirmationStatus']=='Denied':
                logger.info('Responding with: dialogAction type Close')
                return close({'start_from': 0}, 'Failed',
                {'contentType': 'PlainText', 'content': 'Thanks. You can start over by typing *Rate a talk*'})
            else:
                save_data(get_full_session(session_id, session_score, event), record_id)
                logger.info('Responding with: dialogAction type Close')
                return close({'start_from': 0}, 'Fulfilled',
                {'contentType': 'PlainText', 'content': 'Thank you for rating the session!'})
        else:
            slots = get_session_details(session_id)
            event['currentIntent']['slots']['sessionName'] = slots['topic']
            event['currentIntent']['slots']['sessionDate'] = slots['date']
            logger.info('Responding with: dialogAction type Delegate')
            return delegate(None, event['currentIntent']['slots'])
