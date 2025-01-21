import functions_framework
import json
import os
import io
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from datetime import *
import base64
import sib_api_v3_sdk
from sib_api_v3_sdk.rest import ApiException
from google.cloud import secretmanager_v1
from pprint import pprint
from google.cloud import storage
from variables import *


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
file_path='/tmp/report.xlsx'
def lambda_handler(response):
    print(response.headers,"headers")
    print(response,"response")
    report("audit_report/latest_dscr_report.xlsx",'DSCR')
    report("audit_report/latest_bridge_report.xlsx",'Bridge')
    print("test1...")
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
    
def export_excel(df):
    with io.BytesIO() as buffer:
        writer = pd.ExcelWriter(buffer)
        df.to_excel(writer)
        writer.close()
        return buffer.getvalue()
        

def report(path, report_type):
    
    file_name = f'Audit {report_type} Report - {date_for_mail}'
    storage_client = storage.Client(project="toorak-396910")
    i= path
    bucket = storage_client.get_bucket("dw-prod-bronze-purchased-loan-us-es4-gcs")
    blob = bucket.blob(f'{i}')
    content=io.BytesIO(blob.download_as_string())
    print(content)
    df = pd.read_excel(content,index_col=False)
    sender_ = {"name":"noreply@toorakcapital.com","email":"noreply@toorakcapital.com"}
    recipients_ = [{"email":"hsharma@toorakcapital.com","name":"Harsh"}]
    current_time = datetime.now().time()
    cutoff_time = time(12, 00) # 5:30 IST
    if current_time >= cutoff_time:
        recipients_ = [{"email":"asimanovsky@toorakcapital.com","name":"asimanovsky"},
                   {"email":"jkuppuswamy@toorakcapital.com","name":"jkuppuswamy"},
                   {"email":"kaleemuddin.m@toorakcapital.com","name":"kaleemuddin"},
                   {"email":"lsrinivas@toorakcapital.com","name":"lsrinivas"},
                   {"email":"sbantu@toorakcapital.com","name":"bantu"},
                   {"email":"rchannamoni@toorakcapital.com","name":"ravinder"},
                   {"email":"roshans@triconinfotech.com","name":"roshans"},
                   {"email":"ldivya@toorakcapital.com","name":"lila"},
                   {"email":"edyner@toorakcapital.com","name":"edyner"},
                   {"email":"svenugopal@toorakcapital.com","name":"svenugopal"},
                   {"email":"toorakdatateam@triconinfotech.com","name":"toorakdatateam"},
                   {"email":"lberger@toorakcapital.com","name":"lberger"},
                   {"email":"hsharma@toorakcapital.com","name":"Harsh"}]
        print("PROD SENT")
    else:
        recipients_ = [{"email":"hsharma@toorakcapital.com","name":"Harsh"},
                       {"email":"vijaylaxmi@triconinfotech.com","name":"Laxmi"}]
        print("DEV SENT")


    title_ = file_name
    
    text_ = 'The text version\nwith multiple lines.'
    body_ = f"""<html>
              <head>
                <ul id="ul-img" style="list-style-type: none;margin: 0;padding: 0;background-color: #0fcbef;width: 100%;height: 60px;border-radius: 4px;background-image: -webkit-linear-gradient(97deg, #0fcbef 4%, #1071ee 90%) !important;">
                  <div>
                    <img src="https://file-management-dev-tl-ue1-s3.s3.amazonaws.com/tooraklogo.png" />
                  </div>
                </ul>
            </head>
            <body>
            <br>Hi All,<br><br>Please find attached Audit {report_type} Report for date_for_sending_email.<br>Please let us know if you face any issues.<br><br>Thanks,<br>Toorak.<br>
            <img src='https://static.wixstatic.com/media/74c4f9_20406c39aa61491d83b0ccec086c6feb~mv2_d_4500_4500_s_4_2.png/v1/fit/w_1000%2Ch_1000%2Cal_c/file.png' width="250" height="190">
            </body>
            </html>"""
    body_ = body_.replace('date_for_sending_email', date_for_mail)
    attachments_ = export_excel(df)
    
    
    response=send_mail(sender_, recipients_, title_, text_, body_, attachments_,file_name)

    
def send_mail(sender, recipients, title, text, html, attachments,filename):
    """
    Send email to recipients. Sends one mail to all recipients.
    The sender needs to be a verified email in SES.
    """
    configuration = sib_api_v3_sdk.Configuration()
    configuration.api_key['api-key'] = get_secret(secret_name['email_api'])
    api_instance = sib_api_v3_sdk.TransactionalEmailsApi(sib_api_v3_sdk.ApiClient(configuration)) 
    headers = {"Content-Disposition":"Attachments"}

    encoded_string = base64.b64encode(attachments)
    base64_message = encoded_string.decode('utf-8')
    filename+=".xlsx"
    attachment = [{"content":base64_message,"name":filename}]
    send_smtp_email = sib_api_v3_sdk.SendSmtpEmail(to=recipients, html_content=html, sender=sender, subject=title,headers=headers,attachment=attachment)

    try:
        api_response = api_instance.send_transac_email(send_smtp_email)
        pprint(api_response)
        return api_response
    except ApiException as e:
        print("Exception when calling SMTPApi->send_transac_email: %s\n" % e)
        return e
