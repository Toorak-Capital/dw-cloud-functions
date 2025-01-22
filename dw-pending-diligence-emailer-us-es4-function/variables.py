import os
from datetime import datetime
env = os.environ.get('stage', 'dev')
project_id = os.environ.get('project_id', 'toorak-396910')
sender_email = {"name":"noreply@toorakcapital.com","email":"noreply@toorakcapital.com"}
look_ids = {'pending diligence': 103}

if env == 'dev':
    log_bucket_name = f"dw-{env}-cron-job-log-file-execution"
    destination_bucket = f'dw-{env}-bronze-purchased-loan-us-es4-gcs'

    email_type = 'Test'

    CAEmailRecipients = [{"email":'vijaylaxmi@triconinfotech.com',"name":"laxmi"},
                            {"email": "mpinnamaneni@toorakcapital.com", "name": "Mounika Pinnamaneni"}]
    
    CAEmailRecipients_cc = [{"email":'toorakdatateam@triconinfotech.com',"name":"Data Team"}]


    OpusEmailRecipients = [{"email":'vijaylaxmi@triconinfotech.com',"name":"laxmi"},
                            {"email": "mpinnamaneni@toorakcapital.com", "name": "Mounika Pinnamaneni"}]
    
    OpusEmailRecipients_cc = [{"email":'toorakdatateam@triconinfotech.com',"name":"Data Team"}]

    
    secret_name = {'email_api' : 'dw-np-noreply-email-api-key-sm-us-ct1',
                   'looker_creds' : 'looker-creds-dev-dw-ue1-sm'}
    
    secret_project_id = 280549663966
    
else:
    log_bucket_name = f"dw-{env}-cron-job-log-file-execution"
    destination_bucket = f'dw-{env}-bronze-purchased-loan-us-es4-gcs'

    email_type = ''
    
    CAEmailRecipients = [{"email":'ZGribben@ca-usa.com',"name":"ZGribben"},
                        {"email":'WTai@ca-usa.com',"name":"WTai"},
                        {"email":'JMaclennan@ca-usa.com',"name":"JMaclennan"},
                        {"email":'DGaddy@ca-usa.com',"name":"DGaddy"},
                        {"email":'hgottlieb@toorakcapital.com',"name":"hanna"},
                        {"email":'soverton@toorakcapital.com',"name":"Sherry"},
                        {"email":'jkuppuswamy@toorakcapital.com',"name":"Jaya"},
                        {"email":'kaleemuddin.m@toorakcapital.com',"name":"kaleem"},
                        {"email":'mpark@ca-usa.com',"name":"mpark"},
                        {"email":'caanalystsreporting@ca-usa.com',"name":"caanalystsreporting"},
                        {"email":'EDurets@ca-usa.com',"name":"EDurets"},
                        {"email":'ToorakRTL@ca-usa.com',"name":"TooralRTL"},
                        {"email":'ToorakDSCR@ca-usa.com',"name":"ToorakDSCR"}]

    CAEmailRecipients_cc = [{"email":'toorakdatateam@triconinfotech.com',"name":"Data Team"}]
    
    OpusEmailRecipients = [{"email":'daniel.chang@wipro.com',"name":"daniel"},
                            {"email":'jkuppuswamy@toorakcapital.com',"name":"jaya"},
                            {"email":'hgottlieb@toorakcapital.com',"name":"hanna"},
                            {"email":'soverton@toorakcapital.com',"name":"sherry"},
                            {"email":'kaleemuddin.m@toorakcapital.com',"name":"kaleemuddin"}]

    OpusEmailRecipients_cc = [{"email":'toorakdatateam@triconinfotech.com',"name":"Data Team"}]
    
    secret_name = {'email_api' : 'dw-noreply-email-api-key',
                   'looker_creds' : 'looker-creds-prod-dw-ue1-sm'}
    
    secret_project_id = 947624841920
