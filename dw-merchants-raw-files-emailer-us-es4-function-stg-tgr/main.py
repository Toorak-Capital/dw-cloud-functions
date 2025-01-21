import functions_framework
import io
import json
import base64
import datetime
from datetime import date

import pandas as pd
from google.cloud import storage
from google.cloud import secretmanager
import sib_api_v3_sdk
from sib_api_v3_sdk.rest import ApiException

from variables import *

# Imports the Cloud Logging client library
import google.cloud.logging
# Instantiates a client
client = google.cloud.logging.Client()
client.setup_logging()
import logging


today = datetime.datetime.today()
idx = (today.weekday() + 1) % 7 
# temp change
previous_sunday_date = (today - datetime.timedelta(idx+3))


def get_secret(secret_id, version_id="latest"):
    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version.
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version.
    response = client.access_secret_version(request={"name": name})

    return response.payload.data.decode("UTF-8")
 
    

def send_mail(title, html, attachments):
    """
    Send email to recipients. Sends one mail to all recipients.
    The sender needs to be a verified email in SES.
    """
    configuration = sib_api_v3_sdk.Configuration()
    configuration.api_key['api-key'] = get_secret(secret_name['email_api'])
    api_instance = sib_api_v3_sdk.TransactionalEmailsApi(sib_api_v3_sdk.ApiClient(configuration)) 

    headers = {"Content-Disposition":"Attachments"}
    attachment = []
    logging.info(attachments)
    for file_name in attachments:
        logging.info(file_name)
        logging.info(type(attachments[file_name]))
        encoded_string = base64.b64encode(attachments[file_name])
        logging.info(type(encoded_string))
        base64_message = encoded_string.decode('utf-8')
        attachment.append({"content":base64_message, "name":file_name})

    send_smtp_email = sib_api_v3_sdk.SendSmtpEmail(to=email_recipients, html_content=html, sender=sender_email, subject=title, headers=headers,attachment=attachment)

    try:
        api_response = api_instance.send_transac_email(send_smtp_email)
        logging.info(api_response)
        return api_response
    except ApiException as e:
        logging.info("Exception when calling SMTPApi->send_transac_email: %s\n", e)
        return e


@functions_framework.cloud_event
def trigger_on_bsi_merchants_upload(cloudevent):
    '''
    '''
    payload = cloudevent.data.get("protoPayload")
    file_path = payload.get('resourceName', '')
    logging.info("file_path: %s", file_path)
    
    storage_client = storage.Client()  
    blobs = storage_client.list_blobs(SourceBucket, prefix=SourcePrefix)

    attachments_ = {}
    key_list = [blob.name for blob in blobs if previous_sunday_date <= blob.updated.replace(tzinfo=None)]
    
    if len(key_list) < 4 or 'YTD PRODUCTION' not in file_path.upper():
        logging.info('Key list is not met the expected count %s:', key_list)
        return {
            'statusCode': 400,
            'body': json.dumps(f'Key list is not met the expected count : {key_list}')
        }

    for file_path in key_list:
        file_name = file_path.split('/')[-1]

        if file_name.endswith('.xlsx'):
            key = f'bsi_merchants_sftp/daily-extracts/{file_name}'
            csv_filename = key.replace('xlsx','csv').replace('daily-extracts', 'csv-converted')

            logging.info('csv_filename: %s; file_name:%s', csv_filename, file_name)

            dest_key = f'/tmp/{file_name}'
            bucket = storage_client.get_bucket(SourceBucket)
            blob = bucket.blob(file_path)
            blob.download_to_filename(dest_key)

            bucket = storage_client.bucket(DestBucket)
            blob = bucket.blob(key)
            blob.upload_from_filename(dest_key)
            logging.info('Excel file stored in GCS')

            with open(dest_key, "rb") as excel_file:
                ftp_file = excel_file.read()
                attachments_[file_name]= ftp_file
                logging.info(type(ftp_file))
                logging.info('Read file from local')

                content = io.BytesIO(ftp_file)
                logging.info('Read as buffer')

                data_frame = pd.read_excel(content, engine='openpyxl')
                csv_buffer = io.StringIO()
                data_frame.to_csv(csv_buffer, index=False)
                logging.info('dataframe created')

                bucket = storage_client.bucket(DestBucket)
                blob = bucket.blob(csv_filename)
                blob.upload_from_string(csv_buffer.getvalue())  
                logging.info('CSV file stored in GCS')

    logging.info(attachments_)                  

    logging.info('job added')

    title_ = f'{email_type} Merchant Files on {(date.today()).strftime("%m-%d-%Y")}'.lstrip()
    body_ = """<html>
                <head>
                <ul id="ul-img" style="list-style-type: none;margin: 0;padding: 0;background-color: #0fcbef;width: 100%;height: 60px;border-radius: 4px;background-image: -webkit-linear-gradient(97deg, #0fcbef 4%, #1071ee 90%) !important;">
                    <div>
                    <img src="https://file-management-dev-tl-ue1-s3.s3.amazonaws.com/tooraklogo.png" />
                    </div>
                </ul>
            </head>
            <body>
            <br>Hi All,<br><br>Please find attached Merchant files on today.<br>Please let us know if you face any issues.<br><br>Thanks,<br>Toorak.<br>
            <img src='https://static.wixstatic.com/media/74c4f9_20406c39aa61491d83b0ccec086c6feb~mv2_d_4500_4500_s_4_2.png/v1/fit/w_1000%2Ch_1000%2Cal_c/file.png' width="250" height="190">
            </body>
            </html>"""
    body_ = body_.replace('today',str((date.today()).strftime("%m-%d-%Y")))

    if not attachments_:
        logging.error('No email attachment is found')
        raise Exception('No email attachment is found')
    
    send_mail(title_, body_, attachments_)

    logging.info('Successfully sent email')
    return {
        'statusCode': 200,
        'body': json.dumps('Successfully sent email')
    }
  