import os
env = os.environ.get('stage', 'dev')
project_id = os.environ.get('project_id', 'np-toorak')

SourceBucket = f'trk-{env}-sftp-tl-us-es4-gcs'
SourcePrefix = 'BSI-Merchants/reports'
DestBucket = f'dw-{env}-raw-snapshot-us-es4-gcs'

sender_email = {"name":"noreply@toorakcapital.com", "email":"noreply@toorakcapital.com"}

if env == 'prod':
    secret_name = {'email_api' : 'dw-noreply-email-api-key',
                   'looker_creds' : 'looker-creds-prod-dw-ue1-sm'}
    email_recipients = [
        {"email":"asimanovsky@toorakcapital.com", "name":"Aleksandra Simanovsky"},
        {"email":"mbergamaschi@toorakcapital.com", "name":"Mauricio Bergamaschi"},
        {"email":"vijaylaxmi@triconinfotech.com", "name":"vijaylaxmi"},
        {"email":"sasi.jyothsna@triconinfotech.com", "name":"Sasi Jyothsna"},
        {"email":"praveenkumar.vs@triconinfotech.com", "name":"Praveen Kumar"},
        {"email": "mpinnamaneni@toorakcapital.com", "name": "Mounika Pinnamaneni"},
        {"email":"ToorakDataTeam@triconinfotech.com", "name":"ToorakData"}]
else:
    secret_name = {'email_api' : 'dw-np-noreply-email-api-key-sm-us-ct1',
                   'looker_creds' : 'looker-creds-dev-dw-ue1-sm'}
    email_recipients = [
        {"email":"sasi.jyothsna@triconinfotech.com","name":"Sasi Jyothsna"}, 
        {"email":"praveenkumar.vs@triconinfotech.com","name":"Praveen"}, 
        {"email":'vijaylaxmi@triconinfotech.com',"name":"laxmi"}
        ]