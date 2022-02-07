import json
from datetime import datetime, timedelta, timezone
from google.cloud import storage
from google.oauth2 import service_account


service_account_info = json.load(open('cloud_storage_credentials.json'))
credentials = service_account.Credentials.from_service_account_info(service_account_info)
client = storage.Client(
    credentials=credentials,
    project=credentials.project_id,
)


def bqloader(event, context):
    print('Event ID: {}'.format(context.event_id))
    print('Event type: {}'.format(context.event_type))
    print('Bucket: {}'.format(event['bucket']))
    print('File: {}'.format(event['name']))
    print('Metageneration: {}'.format(event['metageneration']))
    print('Created: {}'.format(event['timeCreated']))
    print('Updated: {}'.format(event['updated']))

    bucket = client.get_bucket(event['bucket'])
    blob = bucket.blob(event['name'])
    blob.download_to_filename('/tmp/' + event['name'])
