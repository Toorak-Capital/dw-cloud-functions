import os
import pandas as pd
import uuid
import json
from datetime import *

# Imports the Cloud Logging client library
import google.cloud.logging
# Instantiates a client
client = google.cloud.logging.Client()
# Retrieves a Cloud Logging handler based on the environment
# you're running in and integrates the handler with the
# Python logging module. By default this captures all logs
# at INFO level and higher
client.setup_logging()
import logging

env = os.environ.get('stage', 'dev')

csv_bucket_name_destination = f'dw-{env}-raw-snapshot-us-es4-gcs'
destination_bucket = f'dw-{env}-bronze-purchased-loan-us-es4-gcs'


def read_csv(location):
    '''
    '''
    return pd.read_csv(location, dtype=str)


def read_excel(location, sheet_name = ''):
    '''
    '''
    if sheet_name:
        return pd.read_excel(location, dtype=str, sheet_name = sheet_name)
    return pd.read_excel(location, dtype=str)


def write_parquet_file(df, folderName, source, formatted_date):
    '''
    '''
    parquet_unique_id = 'part-00000-' + str(uuid.uuid4())
    df.to_parquet(f"gs://{destination_bucket}/{source}/to-process-v2/{folderName}/ingestion_date={formatted_date}/{parquet_unique_id}.snappy.parquet", compression='snappy')


def write_parquet_by_date(df, source, folder_name, formatted_date):
    '''
    '''
    parquet_unique_id = f'part-00000-{formatted_date}'
    df.to_parquet(f"gs://{destination_bucket}/{source}/to-process-v2/{folder_name}/ingestion_date={formatted_date}/{parquet_unique_id}.snappy.parquet", compression='snappy')
