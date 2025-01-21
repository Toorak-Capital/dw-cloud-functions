import os
from datetime import datetime
env = os.environ.get('stage', 'dev')
project_id = os.environ.get('project_id', 'toorak-396910')
sender_email = {"name":"noreply@toorakcapital.com","email":"noreply@toorakcapital.com"}
look_ids = {'purchased loans': 112}

if env == 'dev':
    destination_bucket = f'dw-{env}-bronze-purchased-loan-us-es4-gcs'
    email_recipients = [{"email":"sasi.jyothsna@triconinfotech.com","name":"Sasi Jyothsna"},
                    {"email": "mpinnamaneni@toorakcapital.com", "name": "Mounika Pinnamaneni"},
                   {"email":'vijaylaxmi@triconinfotech.com',"name":"laxmi"},
                   {"email":'toorakdatateam@triconinfotech.com',"name":"Data Team"}]
    
    secret_name = {'email_api' : 'dw-np-noreply-email-api-key-sm-us-ct1',
                   'looker_creds' : 'looker-creds-dev-dw-ue1-sm'}

    email_type = 'Test'
    
    secret_project_id = 280549663966
    
else:
    destination_bucket = f'dw-{env}-bronze-purchased-loan-us-es4-gcs'
    email_recipients = [{"email":'lberger@toorakcapital.com',"name":"lberger"},
                            {"email":'kchen@toorakcapital.com  ',"name":"kchen"},
                            {"email":'edyner@toorakcapital.com',"name":"edyner"},
                            {"email":'jward@toorakcapital.com',"name":"jward"},
                            {"email":'svenugopal@toorakcapital.com',"name":"sachin"},
                            {"email":'asimanovsky@toorakcapital.com',"name":"alexandra"},
                            {"email":'mbergamaschi@toorakcapital.com',"name":"mbergamaschi"},
                            {"email":'vijaylaxmi@triconinfotech.com',"name":"laxmi"},
                            {"email": "mpinnamaneni@toorakcapital.com", "name": "Mounika Pinnamaneni"},
                            {"email":'toorakdatateam@triconinfotech.com',"name":"Data Team"}]
    
    secret_name = {'email_api' : 'dw-noreply-email-api-key',
                   'looker_creds' : 'looker-creds-prod-dw-ue1-sm'}

    email_type = ''
    
    secret_project_id = 947624841920
