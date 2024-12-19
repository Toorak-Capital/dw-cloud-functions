import functions_framework
import json
import uuid
from datetime import datetime
import pandas as pd

from variables import *


def getBucketName(event):
    '''
    '''
    bucketName = event["resourceName"].split("/")[3]
    return bucketName


def getkeyValue(event):
    '''
    '''
    keyValue = "/".join((event["resourceName"].split("/"))[5:])
    return keyValue


def read_csv(location):
    '''
    '''
    return pd.read_csv(location)


def write_parquet_file(df, input_table):
    '''
    '''
    output_table = 'hc_' + input_table

    for column in df.columns:
        df[column] = df[column].astype(str)

    df['valid_from'] = datetime.now()
    df['isactive'] = True
    ingestion_date = datetime.now().date()

    parquet_unique_id = 'part-00000-' + str(uuid.uuid4())
    df.to_parquet(f"gs://{destination_bucket}/house_canary/{output_table}/ingestion_date={ingestion_date}/{parquet_unique_id}.snappy.parquet", compression='snappy')


@functions_framework.cloud_event
def trigger_on_house_canary(cloudevent):
    '''
    '''
    payload = cloudevent.data.get("protoPayload")    
    logging.info('payload : %s', payload)

    event = {"resourceName": payload.get('resourceName')} 
    logging.info('event : %s', payload)

    source_bucket = getBucketName(event)
    file_path = getkeyValue(event)

    try:
        tables = ['block_group_distribution', 'sales_history', 'listing_status', 'property_info', 'value_info', 'test_tbd']
        input_table = [table for table in tables  if table in file_path][0]
    except IndexError:
        logging.info('table not found in  the given list: table: %s', input_table)
        return
    
    logging.info('source_bucket:%s ; file_path:%s ; input_table:%s', source_bucket, file_path, input_table)
    
    df = read_csv(f"gs://{source_bucket}/{file_path}")
    write_parquet_file(df, input_table)

    return {
        'statusCode': 200,
        'body': json.dumps('successfuly parsed and write the output')
    }
    
