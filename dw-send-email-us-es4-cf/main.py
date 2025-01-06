import functions_framework
from google.cloud import storage
import base64
import json
import sib_api_v3_sdk
from sib_api_v3_sdk.rest import ApiException
from pprint import pprint 
from google.cloud import secretmanager_v1
from variables import *

@functions_framework.http
def send_email(request):
    request_json: Dict[str, Any] = request.get_json(silent=True)
    subject = request_json['subject']
    recipients = request_json['to']
    body = request_json['body']
    sender = {"name":"noreply@toorakcapital.com","email":"noreply@toorakcapital.com"}
    response=send_mail(sender, recipients, subject, body, attachments = None,filename = "")
    pprint(response)
    return {
        'statusCode': 200,
        'body': json.dumps('Email has been sent successfully')
    }


def send_mail(sender, recipients, title, html, attachments,filename):
    """
    Send email to recipients. Sends one mail to all recipients.
    The sender needs to be a verified email in SES.
    """
    configuration = sib_api_v3_sdk.Configuration()
    configuration.api_key['api-key'] = get_secret(secret_name['email_api'])
    api_instance = sib_api_v3_sdk.TransactionalEmailsApi(sib_api_v3_sdk.ApiClient(configuration)) 

    headers = {"Content-Disposition":"Attachments"}
    send_smtp_email = sib_api_v3_sdk.SendSmtpEmail(to=recipients, html_content=html, sender=sender, subject=title,headers=None,attachment=None)
    try:
        api_response = api_instance.send_transac_email(send_smtp_email)
        print(api_response)
        return api_response
    except ApiException as e:
        print("Exception when calling SMTPApi->send_transac_email: %s\n" % e)
        return e



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