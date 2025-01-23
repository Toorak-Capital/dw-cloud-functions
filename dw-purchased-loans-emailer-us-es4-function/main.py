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
    
today = date.today() # or you can do today = date.today() for today's date
start_date = (today - timedelta(days=7)).strftime("%m-%d-%Y")
end_date = (today - timedelta(days=3)).strftime("%m-%d-%Y")
file_path='/tmp/report.xlsx'

date_for_mail = custom_strftime('%B {S}, %Y', datetime.now())
file_name = f'{email_type} Purchased Loans - {start_date} to {end_date}'.lstrip()

storage_client = storage.Client(project = project_id)

bucket = storage_client.get_bucket(destination_bucket)
get_bucket = storage_client.get_bucket(destination_bucket)

values_list = []


def lambda_handler(response):
    open(file_path, "w").close()
    print('file erased')
    purchased_loans_report()
    print("test1...")
    open(file_path, "w").close()
    print('file erased')

    return {
        'statusCode': 200,
        'body': json.dumps('purchased loans report sent successfully')
    }
    
def dollar_sign(df,float_columns):
    
    # Add $ sign to float columns
    for col in float_columns:
        df[col] = df[col].astype(float)
        df[col] = df[col].apply(lambda x: f'${x:,.2f}')
    return df

def purchased_loans_report():

    looker_creds = get_secret(secret_name['looker_creds'])
    os.environ['LOOKERSDK_BASE_URL'] = looker_creds['LOOKERSDK_BASE_URL']
    os.environ['LOOKERSDK_CLIENT_ID'] = looker_creds['LOOKERSDK_CLIENT_ID']
    os.environ['LOOKERSDK_CLIENT_SECRET'] = looker_creds['LOOKERSDK_CLIENT_SECRET']
    
    sdk = looker_sdk.init40()

    #pending diligence sheet
    response = sdk.run_look(str(look_ids['purchased loans']), "csv")
    purchased_loan = pd.read_csv(io.StringIO(response))
    purchased_loan.columns = [col if 'Purchased Loans Weekly' not in col else col.replace('Purchased Loans Weekly ','') for col in purchased_loan.columns]
    purchased_loan = purchased_loan[['Originator Loan ID','Toorak Loan ID','Loan Type','Originator Name','Closing Date','Origination Date',
    'Purchase Price','Initial Loan Amount','Original Maximum Loan Amount','As Is Ltv Initial','Loan Purpose','Original As Repaired Ltv',
    'Fico','Toorak Interest Rate','Toorak Yield','Property Address','City','State','Zip Code',
    'Property Type','Toorak Product','Kkr Product Category']]
    purchased_loan.rename(columns={'Originator Loan ID': 'Seller Loan ID', 'Toorak Loan ID': 'Loan ID', 'Originator Name': 'Originator', 'Initial Loan Amount': 'Min UPB', 'Original Maximum Loan Amount': 'Max UPB', 'As Is Ltv Initial': 'As-is LTV Initial', 
                                    'Toorak Interest Rate': 'Rate', 'Property Address': 'Address','Original As Repaired Ltv':'ARLTV'}, inplace=True)
    purchased_loan = dollar_sign(purchased_loan,['Purchase Price','Min UPB','Max UPB'])
    purchased_loan = purchased_loan.fillna('')

    with pd.ExcelWriter(file_path) as writer1:
        purchased_loan.to_excel(writer1, sheet_name = 'KKR Dataset', index = False)
        
        
    with open(file_path, 'rb') as f:
        txt=f.read()
        print(type(txt))
        print('read the file')
        # s3_connection.put_object(Body=test, Bucket='prefund-custody-report', Key='purchased_loans_report.xlsx')
        attachments_=txt
       
    sender_ = sender_email
    if dt.time(12,30) <= dt.datetime.now().time()<= dt.time(14,30):
        print('inside if')
        recipients_ = email_recipients
        cc_ = email_recipients_cc
    else:
        print('inside else')
        recipients_ = email_recipients
        cc_ = email_recipients_cc
    print(recipients_)
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
            <br>Hi All,<br><br>Please find attached Purchased Loans Report from start_date to end_date.<br>Please let us know if you face any issues.<br><br>Thanks,<br>Toorak.<br>
            <img src='https://static.wixstatic.com/media/74c4f9_20406c39aa61491d83b0ccec086c6feb~mv2_d_4500_4500_s_4_2.png/v1/fit/w_1000%2Ch_1000%2Cal_c/file.png' width="250" height="190">
            </body>
            </html>"""
    body_ = body_.replace('start_date', str(start_date)).replace('end_date', str(end_date))
    # attachments_ = export_csv(df)
    response=send_mail(sender_, recipients_, cc_, title_, text_, body_, attachments_,file_name)



def send_mail(sender, recipients, cc, title, text, html, attachments,filename):
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
