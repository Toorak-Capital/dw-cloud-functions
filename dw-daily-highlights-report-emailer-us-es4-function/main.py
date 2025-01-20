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
from typing import cast, Dict, Optional
from looker_sdk import models40
import urllib


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
    
date_for_mail = custom_strftime('%B {S}, %Y', datetime.now())

file_name = f'{email_type} Toorak Daily Highlights Report - {date_for_mail}'.lstrip()

storage_client = storage.Client(project = project_id)

bucket = storage_client.get_bucket(destination_bucket)
get_bucket = storage_client.get_bucket(destination_bucket)

looker_creds = get_secret(secret_name['looker_creds'])
os.environ['LOOKERSDK_BASE_URL'] = looker_creds['LOOKERSDK_BASE_URL']
os.environ['LOOKERSDK_CLIENT_ID'] = looker_creds['LOOKERSDK_CLIENT_ID']
os.environ['LOOKERSDK_CLIENT_SECRET'] = looker_creds['LOOKERSDK_CLIENT_SECRET']

sdk = looker_sdk.init40()

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
    tdr_report()

    return {
        'statusCode': 200,
        'body': json.dumps('TDR report sent successfully')
    }
    
def download_dashboard(
    dashboard: models40.Dashboard,
    style: str = "tiled",
    width: int = 545,
    height: int = 842,
    filters: Optional[Dict[str, str]] = None,
    ):

    
    id = dashboard.id
    task = sdk.create_dashboard_render_task(
        id,
        "pdf",
        models40.CreateDashboardRenderTask(
            dashboard_style=style,
            dashboard_filters= urllib.parse.urlencode(filters) if filters else None,
        ),
        width,
        height,
    )

    if not (task and task.id):
        raise Exception(
            f'Could not create a render task for "{dashboard.title}"'
        )

    # poll the render task until it completes
    elapsed = 0.0
    delay = 0.5  # wait .5 seconds
    while True:
        poll = sdk.render_task(task.id)
        if poll.status == "failure":
            print(poll)
            raise Exception(
                f'Render failed for "{dashboard.title}"'
            )
        elif poll.status == "success":
            break
    print(task)
    result = sdk.render_task_results(task.id)
    print(result)
    filename = f"{dashboard.title}.pdf"
    print(f'Dashboard pdf saved to "tmp/{filename}"')
    blob = bucket.blob(f'looker_report_emails/Pipeline_Dashboard_Email.pdf')
    blob.upload_from_string(result)
    print(f'Dashboard pdf saved to gcs')
    return result

def tdr_report():
    
    #dashboard esthetics
    dashboard_title = "TDH"
    filters = {'Date' : datetime.now().strftime('%Y/%m/%d')}
    pdf_style = "tiled"
    pdf_width = 1280
    pdf_height = 842

    #get dashboard details from api
    dashboard = next(iter(sdk.search_dashboards(id = look_ids['dashboard_id'])), None)

    #TDH report
    result = download_dashboard(dashboard, pdf_style, pdf_width, pdf_height, filters)
    
    
    blob = bucket.blob(f'looker_report_emails/TDR_report.pdf')
    blob.upload_from_string(result)
    print(f'Dashboard pdf saved to gcs')
            
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
            <br>Hi All,<br><br>Please find attached Toorak Daily Highlights Report for date_for_sending_email<br>Please let us know if you face any issues.<br><br>Thanks,<br>Toorak.<br>
            <img src='https://static.wixstatic.com/media/74c4f9_20406c39aa61491d83b0ccec086c6feb~mv2_d_4500_4500_s_4_2.png/v1/fit/w_1000%2Ch_1000%2Cal_c/file.png' width="250" height="190">
            </body>
            </html>"""
    body_ = body_.replace('date_for_sending_email', date_for_mail)
    attachments_= result
    recipients_ = email_recipients
    response_ = send_mail(sender_, recipients_, title_, text_, body_, attachments_, file_name)
        


def send_mail(sender, recipients, title, text, html, attachments,filename):
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
    filename+=".pdf"
    attachment = [{"content":base64_message,"name":filename}]
    send_smtp_email = sib_api_v3_sdk.SendSmtpEmail(to=recipients, html_content=html, sender=sender, subject=title,headers=headers,attachment=attachment)

    #return ses_client.send_raw_email(Source=sender,Destinations=recipients,RawMessage={'Data': msg.as_string()})
    try:
        api_response = api_instance.send_transac_email(send_smtp_email)
        return api_response
    except ApiException as e:
        print("Exception when calling SMTPApi->send_transac_email: %s\n" % e)
        return e
    print('mail sent')
