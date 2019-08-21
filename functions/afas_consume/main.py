import inspect
import logging
import json
import base64
import os

from google.auth.transport import requests
from google.cloud import datastore
from google.oauth2 import id_token


class DBProcessor(object):
    def __init__(self):
        self.client = datastore.Client()
        pass

    def process(self, payload):
        kind, key = self.identity(payload)
        entity_key = self.client.key(kind, key)
        entity = self.client.get(entity_key)

        if entity is None:
            entity = datastore.Entity(key=entity_key)

        self.populate_from_payload(entity, payload)
        self.client.put(entity)

    def identity(self, payload):
        return '', ''

    @staticmethod
    def populate_from_payload(entity, payload):
        for name in payload.keys():
            value = payload[name]
            entity[name] = value


class EmployeeProcessor(DBProcessor):

    def __init__(self):
        DBProcessor.__init__(self)

    @staticmethod
    def selector():
        return 'employee'

    def identity(self, payload):
        return 'AFAS_HRM', payload['email_address']


class DepartmentProcessor(DBProcessor):

    def __init__(self):
        DBProcessor.__init__(self)

    @staticmethod
    def selector():
        return 'department'

    def identity(self, payload):
        return 'Departments', payload['Afdeling']


parsers = {
    EmployeeProcessor.selector(): EmployeeProcessor(),
    DepartmentProcessor.selector(): DepartmentProcessor()
}


selector = os.environ.get('DATA_SELECTOR', 'Required parameter is missed')
verification_token = os.environ['PUBSUB_VERIFICATION_TOKEN']
domain_token = os.environ['DOMAIN_VALIDATION_TOKEN']


def topic_to_datastore(request):
    if request.method == 'GET':
        return '''
             <html>
                 <head>
                     <meta name="google-site-verification" content="{token}" />
                 </head>
                 <body>
                 </body>
             </html>
         '''.format(token=domain_token)

    if request.args.get('token', '') != verification_token:
        return 'Invalid request', 400

    # Verify that the push request originates from Cloud Pub/Sub.
    try:
        bearer_token = request.headers.get('Authorization')
        token = bearer_token.split(' ')[1]

        claim = id_token.verify_oauth2_token(token, requests.Request(),
                                             audience='vwt-digital')
        if claim['iss'] not in [
            'accounts.google.com',
            'https://accounts.google.com'
        ]:
            raise ValueError('Wrong issuer.')
    except Exception as e:
        logging.error(e)
        return 'Invalid token: {}\n'.format(e), 400

    # Extract data from request
    envelope = json.loads(request.data.decode('utf-8'))
    payload = base64.b64decode(envelope['message']['data'])

    # Extract subscription from subscription string
    try:
        subscription = envelope['subscription'].split('/')[-1]
        logging.info(f'Message received from {subscription} [{payload}]')

        if selector in parsers:
            parsers[selector].process(json.loads(payload))

    except Exception as e:
        logging.info('Extract of subscription failed')
        logging.debug(e)
        raise e

    # Returning any 2xx status indicates successful receipt of the message.
    # 204: no content, delivery successfull, no further actions needed
    return 'OK', 204
