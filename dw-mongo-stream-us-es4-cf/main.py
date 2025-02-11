import functions_framework
import os
import json
import pandas as pd
from datetime import datetime, timedelta

from pymongo import MongoClient
from google.cloud import storage

from variables import *


def get_mongo_client():
    '''
    '''
    secretKey = os.environ.get("Mongo_Secret")
    MONGO_URI = json.loads(secretKey)['Mongo_SRV_Record']

    # Connect to MongoDB
    client = MongoClient(MONGO_URI)

    databases = client.list_database_names()
    logging.info('database list: %s', databases)
    
    return client


def get_counsel_collection(client, db, collection):
    '''
    '''
    db = client[db]
    collections = db.list_collection_names()
    logging.info('collection list: %s', collections)

    collection = db[collection]
    return collection


def get_documents(collection, data_from=None):
    '''
        # Query MongoDB
    '''
    if data_from:
        # Query from specific range
        query = {
            "$or": [
                {"createdOn": {"$gte": data_from}},
                {"updatedOn": {"$gte": data_from}}
            ]
        }
        logging.info('Query From: %s', data_from)
        documents = list(collection.find(query))
    else:
        # Query all documents
        documents = list(collection.find())

    logging.info('No of documents: %s', len(documents))
    
    # Flatten nested JSON
    df = pd.json_normalize(documents)
    df = df.astype(str)

    logging.info('DTypes: %s', df.dtypes)
    return df


def get_query_date():
    '''
    '''
    # Initialize GCS client
    storage_client = storage.Client(project=project_id)

    # Get yesterday's date in YYYY-MM-DD format
    yesterday = (datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_folder = f"ingested_date={yesterday.strftime('%Y-%m-%d')}"

    # Get list of all folders in the bucket
    blobs = storage_client.list_blobs(destination_bucket, prefix=gcs_prefix_path)

    # Extract folder names
    available_folders = {blob.name.rstrip("/") for blob in blobs}
    sorted_folders = sorted(available_folders, reverse=True)
    last_folder = sorted_folders[0].split('/')[1] if 'ingested_date' in sorted_folders else None

    data_from = None
    if last_folder == yesterday_folder:
        data_from = datetime.strptime(yesterday_folder, 'ingested_date=%Y-%m-%d')
    elif last_folder:
        data_from = datetime.strptime(last_folder, 'ingested_date=%Y-%m-%d') + timedelta(days=1)
        
    formatted_date = (data_from if data_from else yesterday).strftime("%Y-%m-%d")
    return data_from, formatted_date


def write_parquet_file(df, formatted_date):
    '''
    '''
    parquet_unique_id = f'part-00000-{formatted_date}'
    file_path = f"gs://{destination_bucket}/{gcs_prefix_path}ingested_date={formatted_date}/{parquet_unique_id}.snappy.parquet"
    df.to_parquet(file_path, compression='snappy', index=False)

    logging.info(f'{formatted_date} file is uploaded')


@functions_framework.http
def mongo_stream(request):
    """
    """
    data_from, formatted_date = get_query_date()
    client = get_mongo_client()
    collection = get_counsel_collection(client, counsel_db, counsel_collection)
    df = get_documents(collection, data_from=data_from)

    write_parquet_file(df, formatted_date)
    
    return {
        'statusCode': 200,
        'body': json.dumps('file is uploaded')
    }
