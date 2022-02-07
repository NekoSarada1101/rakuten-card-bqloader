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

    # 列名を変更
    month = int(event['name'][9:11])
    next_month = month + 1
    if next_month > 12:
        next_month = 1

    df.rename(columns={'利用日': 'use_date', '利用店名・商品名': 'use_store_name_products_name', '支払方法': 'payment_methods', '利用金額': 'use_amount',
              '支払手数料': 'payment_charge', '支払総額': 'pay_total_amount', '{}月支払金額'.format(str(month)): 'pay_amount', '{}月繰越残高'.format(str(next_month)): 'brought_forward_balance'}, inplace=True)

    # 日付をyyyy-mm-ddに変更
    df['use_date'] = df['use_date'].str.replace('/', '-')
    print(df)

    table = bq_client.get_table('slackbot-288310.my_dataset.rakuten_card_detail')
    result = bq_client.insert_rows_from_dataframe(table, df)
    print(result)
