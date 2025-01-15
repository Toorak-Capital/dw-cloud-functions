import functions_framework
import looker_sdk
import os
import pandas as pd
import numpy as np
import json
import io
#import boto3
import logging
import boxsdk
from json import loads
from boxsdk import JWTAuth,Client
import os
import base64
#from botocore.exceptions import ClientError
import re
from datetime import datetime
from google.cloud import storage
from variables import *
from google.cloud import secretmanager_v1
import openpyxl

todays_date = datetime.now().strftime("%m-%d-%Y")
wells_file = 'DW - Wells IPS {todays_date}.xlsx'


def query_bigquery():
    client = bigquery.Client()
    result = False

    query = """
        SELECT * FROM reporting.dw_pipeline_log WHERE run_finished_time >= DATETIME(CONCAT(CAST(CURRENT_DATE("Asia/Calcutta") AS STRING), ' 12:30:00')) LIMIT 1
    """

    query_job = client.query(query)

    results = query_job.result()  # Waits for the job to complete
    for row in results:
        date = row['run_finished_time'].date()
        print(date)
        if date == datetime.now().date():
            result = True
    return result

def check_log_file_in_gcs(bucket_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    
    # List blobs in the bucket
    blobs = bucket.list_blobs()
    
    # Check for any .log files
    for blob in blobs:
        if blob.name.endswith('.log'):
            return True  # At least one .log file exists
    
    return False  # No .log files found


def get_secret(secret_id):

    client = secretmanager_v1.SecretManagerServiceClient()
    name = f"projects/{secret_project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(name=name)
    secret_data = response.payload.data.decode("UTF-8")
    try:
        secret_dict = json.loads(secret_data)
        return secret_dict
    except json.JSONDecodeError:
        # If it's not JSON, return the data as a plain string
        return secret_data



@functions_framework.http
def check_pipeline_run(request):

  log_file_exists = check_log_file_in_gcs(log_bucket_name)
  pipeline_ran_today = query_bigquery()

  if log_file_exists or not pipeline_ran_today:
    upload_report_to_box()


def upload_report_to_box():


    box_creds = get_secret(secret_name['box_creds'])
    config = JWTAuth.from_settings_dictionary(box_creds['box_key'])                    
    client = Client(config)
    user_to_impersonate = client.user(user_id=box_creds['box_id'])
    user_client = client.as_user(user_to_impersonate)

    #initiate looker sdk
    looker_creds = get_secret(secret_name['looker_creds'])
    os.environ['LOOKERSDK_BASE_URL'] = looker_creds['LOOKERSDK_BASE_URL']
    os.environ['LOOKERSDK_CLIENT_ID'] = looker_creds['LOOKERSDK_CLIENT_ID']
    os.environ['LOOKERSDK_CLIENT_SECRET'] = looker_creds['LOOKERSDK_CLIENT_SECRET']
    sdk = looker_sdk.init40()

    response = sdk.run_look(look_id['wells_ips'], "csv")
    df = pd.read_csv(io.StringIO(response))
    df.columns = [col if 'Wells Ips' not in col else col.replace('Wells Ips ','') for col in df.columns]


    excel_buffer = io.BytesIO()
    df.to_excel(excel_buffer, index=False, engine='openpyxl')
    excel_buffer.seek(0)
    uploaded_file = user_client.folder(box_folder_id).upload_stream(excel_buffer, file_name=wells_file)
       
    print(f'file {wells_file} uploaded to box')
    