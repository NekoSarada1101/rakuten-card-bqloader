import json

import pandas as pd
from google.cloud import bigquery, storage
from google.oauth2 import service_account

gsc_service_account_info = json.load(open('cloud_storage_credentials.json'))
gcs_credentials = service_account.Credentials.from_service_account_info(gsc_service_account_info)
gcs_client = storage.Client(
    credentials=gcs_credentials,
    project=gcs_credentials.project_id,
)

bq_service_account_info = json.load(open('bigquery_credentials.json'))
bq_credentials = service_account.Credentials.from_service_account_info(bq_service_account_info)
bq_client = bigquery.Client(
    credentials=bq_credentials,
    project=bq_credentials.project_id,
)


def bqloader(event, context):
    print('Event ID: {}'.format(context.event_id))
    print('Event type: {}'.format(context.event_type))
    print('Bucket: {}'.format(event['bucket']))
    print('File: {}'.format(event['name']))
    print('Metageneration: {}'.format(event['metageneration']))
    print('Created: {}'.format(event['timeCreated']))
    print('Updated: {}'.format(event['updated']))

    bucket = gcs_client.get_bucket(event['bucket'])
    blob = bucket.blob(event['name'])
    blob.download_to_filename('/tmp/' + event['name'])

    df = pd.read_csv('/tmp/' + event['name'], header=0)
    df.drop(columns=['利用者', '新規サイン'], inplace=True)

    table = bq_client.get_table('slackbot-288310.my_dataset.rakuten_card_detail')
    bq_client.insert_rows_from_dataframe(table, df)
