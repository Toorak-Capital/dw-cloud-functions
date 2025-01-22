import functions_framework
import io
from PIL import Image
import looker_sdk
import os
import pandas as pd
import numpy as np
from datetime import datetime
import time
import openpyxl
from openpyxl.utils import get_column_letter
from openpyxl.styles import Alignment
from google.cloud import storage
from google.cloud import bigquery
import json
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import requests
import base64
import sib_api_v3_sdk
from sib_api_v3_sdk.rest import ApiException
from google.cloud import secretmanager_v1
import jinja2
import datetime as dt
from variables import *


start_time = time.time()
def suffix(d):
    return 'th' if 11<=d<=13 else {1:'st',2:'nd',3:'rd'}.get(d%10, 'th')

def custom_strftime(format, t):
    return t.strftime(format).replace('{S}', str(t.day) + suffix(t.day))

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
    
def check_log_file_in_gcs(bucket_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    
    blobs = bucket.list_blobs()
    
    # Check for any .log files
    for blob in blobs:
        if blob.name.endswith('.log'):
            return True  # At least one .log file exists
    
    return False  # No .log files found

def query_bigquery():
    # Initialize BigQuery client
    client = bigquery.Client()
    result = False
    
    # Define your SQL query
    query = """
        SELECT * FROM reporting.dw_pipeline_log WHERE run_finished_time >= DATETIME(CONCAT(CAST(CURRENT_DATE("Asia/Calcutta") AS STRING), ' 12:30:00')) LIMIT 1
    """
    
    # Run the query
    query_job = client.query(query)
    
    # Get the results
    results = query_job.result()  # Waits for the job to complete
    for row in results:
        date = row['run_finished_time'].date()
        print(date)
        if date == datetime.now().date():
            result = True
    return result   

date_for_mail = custom_strftime('%B {S}, %Y', datetime.now())
ca_file_path='/tmp/ca_file.xlsx'
opus_file_path='/tmp/opus_file.xlsx'

file_name = f'{email_type} Pending Diligence Action Report - {date_for_mail}'.lstrip()

storage_client = storage.Client(project = project_id)

bucket = storage_client.get_bucket(destination_bucket)
get_bucket = storage_client.get_bucket(destination_bucket)

values_list = []


def lambda_handler(response):
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
    open(ca_file_path, "w").close()
    print('file erased')
    open(opus_file_path, "w").close()
    print('file erased')
    pending_diligence_report()
    print("test1...")
    open(ca_file_path, "w").close()
    print('file erased')
    open(opus_file_path, "w").close()
    print('file erased')

    return {
        'statusCode': 200,
        'body': json.dumps('pending diligence report sent successfully')
    }

def pending_diligence_report():

    looker_creds = get_secret(secret_name['looker_creds'])
    os.environ['LOOKERSDK_BASE_URL'] = looker_creds['LOOKERSDK_BASE_URL']
    os.environ['LOOKERSDK_CLIENT_ID'] = looker_creds['LOOKERSDK_CLIENT_ID']
    os.environ['LOOKERSDK_CLIENT_SECRET'] = looker_creds['LOOKERSDK_CLIENT_SECRET']
    
    sdk = looker_sdk.init40()

    #pending diligence sheet
    response = sdk.run_look(str(look_ids['pending diligence']), "csv")
    pending_df = pd.read_csv(io.StringIO(response))
    pending_df.columns = [col if 'Pending Diligence Action Report' not in col else col.replace('Pending Diligence Action Report ','') for col in pending_df.columns]
    pending_df = pending_df[['Toorak Loan ID', 'Originator Loan ID', 'Originator', 'Dd Account Name',
        'Loan Purpose', 'Loan State', 'Loan Stage', 'Loan Type',
        'Toorak Product', 'External Exception ID', 'Exception Description',
        'Initial Comment', 'Conclusion Comment', 'Due Diligence Response',
        'Condition Created Date', 'Originator Satisfy Response Date',
        'Satisfy Comment', 'Waiver Approved Date', 'Waiver Final Response',
        'Waiver Approval Comment', 'Due Diligence Response Date',
        'Is Due Diligence Last Response', 'Compensating Factor', 'Box Path']]
    pending_df = pending_df.fillna('')

    ca_df = pending_df[pending_df['Dd Account Name'] == 'Consolidated Analytics']
    opus_df = pending_df[pending_df['Dd Account Name'] == 'Opus']
    ca_df = ca_df.reset_index()
    opus_df = opus_df.reset_index()
    ca_df.index.name='Index'
    opus_df.index.name='Index'
    
    with pd.ExcelWriter(ca_file_path) as writer1:
        ca_df.to_excel(writer1, sheet_name = 'Pending Diligence Action Report', index = False)
    
    with pd.ExcelWriter(opus_file_path) as writer1:
        opus_df.to_excel(writer1, sheet_name = 'Pending Diligence Action Report', index = False)
         
        
    sender_ = sender_email
    title_ = file_name
    
    text_ = 'The text version\nwith multiple lines.'
    body_ = """<html>
              <head>
                <ul id="ul-img" style="list-style-type: none;margin: 0;padding: 0;background-color: #0fcbef;width: 100%;height: 60px;border-radius: 4px;background-image: -webkit-linear-gradient(97deg, #0fcbef 4%, #1071ee 90%) !important;">
                  <div>
                    <img src="https://file-management-dev-tl-ue1-s3.s3.amazonaws.com/tooraklogo.png" />
                  </div>
                </ul>
            </head>
            <body>
            <br>Hi All,<br><br>Please find attached Pending Diligence Action Report for date_for_sending_email<br>Please let us know if you face any issues.<br><br>Thanks,<br>Toorak.<br>
            <img src='https://static.wixstatic.com/media/74c4f9_20406c39aa61491d83b0ccec086c6feb~mv2_d_4500_4500_s_4_2.png/v1/fit/w_1000%2Ch_1000%2Cal_c/file.png' width="250" height="190">
            </body>
            </html>"""
    body_ = body_.replace('date_for_sending_email', date_for_mail)
    
    with open(ca_file_path, 'rb') as f:
        ca_file_data=f.read()
        print(type(ca_file_data))
        print('read the file')
        attachments_=ca_file_data
        recipients_ = CAEmailRecipients
        cc_ = CAEmailRecipients_cc
        response_ = send_mail(sender_, recipients_, cc_, title_, text_, body_, attachments_, file_name)
        
    with open(opus_file_path, 'rb') as f:
        opus_file_data=f.read()
        print('read the file')
        attachments_=opus_file_data
        recipients_ = OpusEmailRecipients
        cc_ = OpusEmailRecipients_cc
        response_ = send_mail(sender_, recipients_, cc_, title_, text_, body_, attachments_, file_name)




def send_mail(sender, cc, recipients, title, text, html, attachments,filename):
    """
    Send email to recipients. Sends one mail to all recipients.
    The sender needs to be a verified email in SES.
    """
    #msg = create_multipart_message(sender, recipients, title, text, html, attachments)
    
#     ses_client = boto3.client('ses',
#                              )

    #ses_client = boto3.client('ses',region_name='us-east-1')
    configuration = sib_api_v3_sdk.Configuration()
    configuration.api_key['api-key'] = get_secret(secret_name['email_api'])
    api_instance = sib_api_v3_sdk.TransactionalEmailsApi(sib_api_v3_sdk.ApiClient(configuration)) 
    #html_title="<html><body><h1>{{title}}</h1></body></html>" 
    headers = {"Content-Disposition":"Attachments"}

    encoded_string = base64.b64encode(attachments)
    base64_message = encoded_string.decode('utf-8')
    filename+=".xlsx"
    attachment = [{"content":base64_message,"name":filename}]
    send_smtp_email = sib_api_v3_sdk.SendSmtpEmail(to=recipients, cc=cc, html_content=html, sender=sender, subject=title,headers=headers,attachment=attachment)

    #return ses_client.send_raw_email(Source=sender,Destinations=recipients,RawMessage={'Data': msg.as_string()})
    try:
        api_response = api_instance.send_transac_email(send_smtp_email)
        return api_response
    except ApiException as e:
        print("Exception when calling SMTPApi->send_transac_email: %s\n" % e)
        return e
    print('mail sent')
