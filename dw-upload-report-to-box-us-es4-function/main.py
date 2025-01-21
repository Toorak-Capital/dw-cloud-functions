import functions_framework
import looker_sdk
import os
import pandas as pd
import numpy as np
import json
import io
import boxsdk
from json import loads
from boxsdk import JWTAuth,Client
import os
import re
from datetime import datetime
from google.cloud import bigquery
from google.cloud import storage
from variables import *
from google.cloud import secretmanager_v1
import openpyxl
from openpyxl import load_workbook
from openpyxl.styles import Alignment
# Imports the Cloud Logging client library
import google.cloud.logging
# Instantiates a client
client = google.cloud.logging.Client()
client.setup_logging()
import logging

todays_date = datetime.now().strftime("%m-%d-%Y")
wells_file_name = f'DW - Wells IPS {todays_date}.xlsx'
pst_file_name = f'DW - Payment Status Tracker {todays_date}.xlsx'

def retriveBoxConfigFromSecret(secret):

    privateKey = secret['privateKey'].replace("\\n","\n")

    json_object = {
      "boxAppSettings": {
        "clientID": secret['clientID'] ,
        "clientSecret": secret['clientSecret'],
        "appAuth": {
          "publicKeyID": secret['publicKeyID'] ,
          "privateKey": privateKey,
          "passphrase": secret['passphrase']
        }
      },
      "enterpriseID": secret['enterpriseID']
    }
    box_user_id = secret['box_user_id']

    return json_object,box_user_id


def query_bigquery():
    client = bigquery.Client()
    result = False

    query = """
        SELECT * FROM reporting.dw_pipeline_log WHERE run_finished_time >= DATETIME(CONCAT(CAST(CURRENT_DATE("Asia/Calcutta") AS STRING), ' 17:30:00')) LIMIT 1
    """

    query_job = client.query(query)

    results = query_job.result()  # Waits for the job to complete
    for row in results:
        date = row['run_finished_time'].date()
        logging.info(date)
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

        logging.info('Either files are missing or Pipeline did not ran. Cannot send emails')
        return {
        'statusCode': 500,
        'body': json.dumps('Either files are missing or Pipeline did not ran. Cannot send emails')
        }
    else:

        box_looker_conn()

        return {
            'statusCode': 200,
            'body': json.dumps({"message": "uploaded all files into box"})
        }

def dollar_sign(df,float_columns):
    
    # Add $ sign to float columns
    
    for col in float_columns:
        if col in list(df.columns):
            logging.info(col)
            df[col] = df[col].astype(float)
            df[col] = df[col].apply(lambda x: f'${x:,.2f}' if pd.notna(x) else np.nan)
    return df
    
def percentage_sign(df,float_columns):
    
    # Add $ sign to float columns
    for col in float_columns:
        if col in list(df.columns):
            logging.info(col)
            df[col] = df[col].astype(float)
            df[col] = df[col].apply(lambda x: f'{x*100:,.2f}%' if pd.notna(x) else np.nan)
    return df

def date_column_format(df,columns):

    for col in columns:
        if col in df.columns:
            logging.info(col)
    
            def safe_to_datetime(date_str):
                try:
                    
                    return pd.to_datetime(date_str, format='%m/%d/%Y').date()
                except pd.errors.OutOfBoundsDatetime:
                    
                    return date_str
                except Exception as e:
                    
                    logging.info(f"Error parsing date '{date_str}': {e}")
                    return date_str
    
            df[col] = df[col].apply(safe_to_datetime)
    
    return df
    
def run_look_and_clean_df(sdk, look_id, col_name):
    
    row_limit = 7000
    response = sdk.run_look(look_id, "csv", limit = row_limit)
    df = pd.read_csv(io.StringIO(response))
    df.columns = [col.replace('_', ' ') if col_name not in col else col.replace(f'{col_name} ','') for col in df.columns]
    
    
    if 'mba order' in df.columns:
        df.drop('mba order', axis = 1, inplace = True)

    df = date_column_format(df, date_columns)
    df = dollar_sign(df, dollar_columns)
    df = percentage_sign(df, percentage_columns)
    
    return df

def pst_file_prep(sdk, user_client, pst_look_ids):


    logging.info('inside pst prep')
    tmp_file = '/tmp/report.xlsx'
    pst_all_loans = run_look_and_clean_df(sdk, pst_look_ids['pst_all_loans'],'Payment Status Tracker')
    all_loans_summary = run_look_and_clean_df(sdk, pst_look_ids['pst_summary'],'Pst Summary')
    bridge_summary = run_look_and_clean_df(sdk, pst_look_ids['pst_bridge_summary'], 'Pst Bridge Summary')
    dscr_summary = run_look_and_clean_df(sdk, pst_look_ids['pst_dscr_summary'],'Pst Dscr Summary')

    empty_column = pd.DataFrame({ ' ': [np.nan] * len(bridge_summary) })

    pst_summary = pd.concat([all_loans_summary,empty_column,empty_column,bridge_summary,empty_column,empty_column,dscr_summary], axis=1)

    with pd.ExcelWriter(tmp_file, engine='openpyxl') as writer:
        pst_summary.to_excel(writer, index=False, header=True, sheet_name='Summary', startrow=1)
        pst_all_loans.to_excel(writer, index=False, header=True, sheet_name='PST')

    wb = load_workbook(tmp_file)
    ws = wb['Summary']

    ws.merge_cells('A1:D1')
    ws['A1'] = 'All Loans'
    ws['A1'].alignment = Alignment(horizontal='center', vertical='center')


    ws.merge_cells('G1:J1')
    ws['G1'] = 'Bridge'
    ws['G1'].alignment = Alignment(horizontal='center', vertical='center')

    ws.merge_cells('M1:P1')
    ws['M1'] = 'DSCR'
    ws['M1'].alignment = Alignment(horizontal='center', vertical='center')

    pst_buffer = io.BytesIO()
    wb.save(pst_buffer)
    upload_file_to_box(user_client, pst_buffer, pst_box_folder_id, pst_file_name)


def wells_file_prep(sdk, user_client, wells_look_id):

    logging.info('inside wells prep')
    wells_df = run_look_and_clean_df(sdk, wells_look_id,'Wells Ips')
    wells_buffer = io.BytesIO()
    wells_df.to_excel(wells_buffer, index=False, engine='openpyxl')
    upload_file_to_box(user_client, wells_buffer, wells_box_folder_id, wells_file_name)
    

def upload_file_to_box(user_client, buffer, box_folder_id, file_name):

    buffer.seek(0)
    uploaded_file = user_client.folder(box_folder_id).upload_stream(buffer, file_name=file_name)
    logging.info(f"{file_name} is uploaded to box successfully")



def box_looker_conn():

    box_creds = get_secret(secret_name['box_creds'])
    box_key,box_user_id = retriveBoxConfigFromSecret(box_creds)
    config = JWTAuth.from_settings_dictionary(box_key)                    
    client = Client(config)
    user_to_impersonate = client.user(user_id=box_user_id)
    user_client = client.as_user(user_to_impersonate)

    #initiate looker sdk
    looker_creds = get_secret(secret_name['looker_creds'])
    os.environ['LOOKERSDK_BASE_URL'] = looker_creds['LOOKERSDK_BASE_URL']
    os.environ['LOOKERSDK_CLIENT_ID'] = looker_creds['LOOKERSDK_CLIENT_ID']
    os.environ['LOOKERSDK_CLIENT_SECRET'] = looker_creds['LOOKERSDK_CLIENT_SECRET']
    sdk = looker_sdk.init40()

    pst_file_prep(sdk, user_client, look_id['pst'])
    wells_file_prep(sdk, user_client, look_id['wells_ips'])
