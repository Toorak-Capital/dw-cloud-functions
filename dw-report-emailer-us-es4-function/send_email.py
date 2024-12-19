import sib_api_v3_sdk
from sib_api_v3_sdk.rest import ApiException
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import base64

def send_mail(sender, recipients, cc, text, html, title, attachments,filename, email_api):
    """
    Send email to recipients. Sends one mail to all recipients.
    The sender needs to be a verified email in SES.
    """
    configuration = sib_api_v3_sdk.Configuration()
    configuration.api_key['api-key'] = email_api
    api_instance = sib_api_v3_sdk.TransactionalEmailsApi(sib_api_v3_sdk.ApiClient(configuration)) 
    #html_title="<html><body><h1>{{title}}</h1></body></html>" 

    filename+=".xlsx"
    print('attachement info')
    print(attachments)
    encoded_string = base64.b64encode(attachments)
    base64_message = encoded_string.decode('utf-8')
    
    if len(base64_message) == 0:
        print('len zero')
        print(html)
        headers = {'Content-Type': 'application/json'}
        send_smtp_email = sib_api_v3_sdk.SendSmtpEmail(to=recipients, cc=cc, html_content=html, sender=sender, subject=title,headers=headers)
        pass
    else:
        headers = {"Content-Disposition":"Attachments"}
        attachment = [{"content":base64_message,"name":filename}]
        send_smtp_email = sib_api_v3_sdk.SendSmtpEmail(to=recipients, cc=cc, html_content=html, sender=sender, subject=title,headers=headers,attachment=attachment)

    print(f"mail sent to {recipients} and cc'ed {cc}")
    try:
        api_response = api_instance.send_transac_email(send_smtp_email)
        return api_response
    except ApiException as e:
        print("Exception when calling SMTPApi->send_transac_email: %s\n" % e)
        return e
