import functions_framework
import os
import json
import uuid
from datetime import datetime
import pandas as pd
from variables import *
import re
import logging
logging.basicConfig(level=logging.DEBUG)


def get_bucket_name(event, bucket_regex_pattern):
    try:
        bucket_name_match = re.search(bucket_regex_pattern, event.get('resourceName', ''))
        if bucket_name_match:
            return bucket_name_match.group(0)
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    return None

def get_file_path(event, file_path_regex_pattern):
    try:
        # Extract the file path using regex
        file_path_match = re.search(file_path_regex_pattern, event.get('resourceName', ''))
        
        # Check if a match is found
        if file_path_match:
            return file_path_match.group(1)
        else:
            logging.warning("No file path found in the 'resourceName'")
            return None
    except Exception as e:
        # Handle any errors gracefully
        logging.error(f"An error occurred while extracting file path: {e}")
        return None

def get_folder_name(input_string, folder_regex_pattern):
    try:        
        # Search for the pattern in the input string
        match = re.search(folder_regex_pattern, input_string)
        
        # Check if a match is found
        if match:
            return match.group(1)
        else:
            logging.warning("No folder name found in the input")
            return None
    except Exception as e:
        # Handle any errors gracefully
        logging.error(f"An error occurred while extracting folder name: {e}")
        return None


def read_csv(location):
    '''
    '''
    return pd.read_csv(location, dtype=str)


def write_parquet_file(df, folderName, formatted_date):
    '''
    '''
    parquet_unique_id = 'part-00000-' + str(uuid.uuid4())
    df.to_parquet(f"gs://{destination_bucket}/breezeway/to-process-v2/{folderName}/ingestion_date={formatted_date}/{parquet_unique_id}.snappy.parquet", compression='snappy')

"""
_summary_:
    This is a function to upload the parquet file to a location
Returns:
    A json structure with status and body
"""
@functions_framework.cloud_event
def trigger_on_breezeway_upload(cloudevent):
    try:
        payload = cloudevent.data.get("protoPayload")    
        event = {"resourceName": payload.get('resourceName')} 
        source_bucket = get_bucket_name(event, bucket_regex_pattern)
        logging.info(source_bucket,"source_bucket")
        file_path = get_file_path(event, file_path_regex_pattern)
        logging.info(file_path,"file_path")
        folder_name = get_folder_name(file_path, folder_regex_pattern)
        logging.info(folder_name,"folder_name")
        df = read_csv(f"gs://{source_bucket}/{file_path}")
        df['data_date'] = data_date_format

        write_parquet_file(df, folder_name, formatted_date)
        response_body = 'Successfully wrote the file'
        status_code = 200
    except Exception as e:
        response_body = f"Error: {str(e)}"
        status_code = 500
    
    return {
        'statusCode': status_code,
        'body': json.dumps(response_body)
    }