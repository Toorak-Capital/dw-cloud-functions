import functions_framework
import io
from PIL import Image
import looker_sdk
import os
import pandas as pd
import numpy as np
from datetime import datetime,date,timedelta
import time
import openpyxl
from openpyxl.utils import get_column_letter
from openpyxl.styles import Alignment
from google.cloud import storage
from google.cloud import bigquery
import json
from openpyxl.styles import Font
import requests
from google.cloud import secretmanager_v1
import jinja2
import datetime as dt
from variables import *
from merchants_emailer.merchants_emailer import *
from twtr_report.twtr_report import *
from uk_emailer.uk_emailer import *
from diligence_emailer.diligence_emailer import *
from risk_score_emailer.risk_score_emailer import *
from table_funding_emailer.table_funding import *
from pst_emailer.pst_emailer import *
from pst_comparator_emailer.pst_comparator_emailer import *
from weekly_credit_metrics_emailer.weekly_credit_metrics import *


start_time = time.time()
def suffix(d):
    return 'th' if 11<=d<=13 else {1:'st',2:'nd',3:'rd'}.get(d%10, 'th')

def custom_strftime(format, t):
    return t.strftime(format).replace('{S}', str(t.day) + suffix(t.day))

today_date = datetime.now()

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
    
date_for_mail = custom_strftime('%B {S}, %Y', datetime.now())
file_path='/tmp/output.xlsx'


storage_client = storage.Client(project = project_id)
bucket = storage_client.get_bucket(destination_bucket)
get_bucket = storage_client.get_bucket(destination_bucket)


looker_creds = get_secret(secret_name['looker_creds'])
os.environ['LOOKERSDK_BASE_URL'] = looker_creds['LOOKERSDK_BASE_URL']
os.environ['LOOKERSDK_CLIENT_ID'] = looker_creds['LOOKERSDK_CLIENT_ID']
os.environ['LOOKERSDK_CLIENT_SECRET'] = looker_creds['LOOKERSDK_CLIENT_SECRET']
sdk = looker_sdk.init40()

email_api = get_secret(secret_name['email_api'])


def find_first_working_day(year, month):
    # Check each day from the 1st to the 7th of the month
    for day in range(1, 8):
        date = datetime(year, month, day)
        if date.weekday() < 5:  # Monday-Friday are 0-4
            return date.date()
    return None

def check_first_working_day():
    today = datetime.now()
    
    # Check if today is the first working day of the month
    if today.day <= 7:
        first_working_day = find_first_working_day(today.year, today.month)
        if today.date() == first_working_day:
            return True

    return False

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


def lambda_handler(request):
    request_json = request.get_json(silent=True)
    if request_json['report'] == 'table_funding':  
            file_name = f'{email_type} Table Funding Pipeline Report - {date_for_mail}'.lstrip()
            response = table_funding_report(file_name, sdk, email_api, bucket, get_bucket)
            
    elif request_json['report'] == 'ca_diligence_submission':
            dd_firm = 'Consolidated Analytics'
            file_name = f'{email_type} Diligence Submission Report - {date_for_mail}'.lstrip()
            response = diligence_submission_emailer(file_name, dd_firm, sdk, email_api, bucket, get_bucket)
            
    elif request_json['report'] == 'opus_diligence_submission':
            dd_firm = 'Opus'
            file_name = f'{email_type} Diligence Submission Report - {date_for_mail}'.lstrip()
            response = diligence_submission_emailer(file_name, dd_firm, sdk, email_api, bucket, get_bucket)
    
    
    log_file_exists = check_log_file_in_gcs(log_bucket_name)
    pipeline_ran_today = query_bigquery()
    
    if log_file_exists or not pipeline_ran_today:
        
        print('Either files are missing or Pipeline did not ran. Cannot send emails')
        return {
        'statusCode': 500,
        'body': json.dumps('Either files are missing or Pipeline did not ran. Cannot send emails')
        }
        
    else: # log file doesn't exist and pipeline ran today
        print("No .log files found in the bucket.")
        
        if request_json:
            print("Received JSON:", request_json)

        if request_json['report'] == 'merchant_weekly':
            file_name = f'{email_type} Merchants Weekly Report - {date_for_mail}'.lstrip()
            response = merchant_weekly_report(file_name, sdk, email_api, bucket, get_bucket)

        elif request_json['report'] == 'twtr_report':
            file_name = f'{email_type} Toorak Weekly Trend Report - {date_for_mail}'.lstrip()
            response = twtr_report(file_name, sdk, email_api, bucket, get_bucket)
        
        elif request_json['report'] == 'weekly_credit_metrics':
            file_name = f'{email_type} Weekly Credit Metrics Report - {date_for_mail}'.lstrip()
            response = weekly_credit_metrics_report(file_name, sdk, email_api, bucket, get_bucket)
            
        elif request_json['report'] == 'uk_weekly':
            file_name = f'{email_type} UK Weekly Report - {date_for_mail}'.lstrip()
            response = uk_weekly_report(file_name, sdk, email_api, bucket, get_bucket)

        elif request_json['report'] == 'pst_daily':
            file_name = f'{email_type} Payment Status Tracker Report - {date_for_mail}'.lstrip()
            response = pst_emailer(file_name, sdk, email_api, bucket, get_bucket)

            file_name = f'{email_type} PST Comparator - {date_for_mail}'.lstrip()
            response = pst_comparator_emailer(file_name, sdk, email_api, bucket, get_bucket)

            file_name = f'{email_type} Originator Performance Report - {date_for_mail}'.lstrip()
            response = opr_emailer(file_name, sdk, email_api, bucket, get_bucket)
        
        
        elif request_json['report'] == 'risk_score_weekly_report':
            frequency = 'weekly' 
            start_date = (today_date - timedelta(days=7)).strftime('%m/%d/%Y')
            end_date = (today_date - timedelta(days=1)).strftime('%m/%d/%Y')
            email_body = f'duration {start_date} to {end_date}'       
            file_name = f'{email_type} Risk Score Report - {date_for_mail}'.lstrip()
            response = risk_score_emailer(file_name, frequency, email_body, sdk, email_api, bucket, get_bucket)
        
        elif request_json['report'] == 'risk_score_monthly_report':
            frequency = 'monthly'
            email_flag = check_first_working_day()
            if email_flag : 
                month_year = custom_strftime('%B-%Y', today_date)
                email_body = f'month of {month_year}'
                file_name = f'{email_type} Risk Score Report - {date_for_mail}'.lstrip()
                response = risk_score_emailer(file_name, frequency, email_body, sdk, email_api, bucket, get_bucket)
            else:
                print("email is not sent cuz it's not the first day of the month or a weekend")
            
        else:
            print('NOTHING')

        return {
            'statusCode': 200,
            'body': f"{request_json['report']} is sent successfully"
        }

