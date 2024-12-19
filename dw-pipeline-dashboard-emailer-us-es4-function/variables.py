import os
from datetime import datetime
env = os.environ.get('stage', 'dev')
project_id = os.environ.get('project_id', 'toorak-396910')
sender_email = {"name":"noreply@toorakcapital.com","email":"noreply@toorakcapital.com"}
look_ids = {'all sheet': 67,
            'POST Close DoD Difference': 57,
            'PRE Close DoD Difference' : 58,
            'PRE POST Close DoD Difference' : 56,
            'PRE Close Submission' : 68,
            'POST Close Submission' : 65,
            'Loan Info' : 64,
            'loans by originator' : 122,
            'loans priciple balance' : 52}

if env == 'dev':
    log_bucket_name = f"dw-{env}-cron-job-log-file-execution"
    destination_bucket = f'dw-{env}-bronze-purchased-loan-us-es4-gcs'
    email_recipients = [{"email":"sasi.jyothsna@triconinfotech.com","name":"Sasi Jyothsna"},
                    {"email": "mpinnamaneni@toorakcapital.com", "name": "Mounika Pinnamaneni"},
                   {"email":'vijaylaxmi@triconinfotech.com',"name":"laxmi"}]
    
    secret_name = {'email_api' : 'dw-np-noreply-email-api-key-sm-us-ct1',
                   'looker_creds' : 'looker-creds-dev-dw-ue1-sm'}
    
    secret_project_id = 280549663966
    
else:
    log_bucket_name = f"dw-{env}-cron-job-log-file-execution"
    destination_bucket = f'dw-{env}-bronze-purchased-loan-us-es4-gcs'
    email_recipients = [{"email":'jbeacham@toorakcapital.com',"name":"john"},
                        {"email":'kparekh@toorakcapital.com',"name":"ketan"},
                        {"email":'svenugopal@toorakcapital.com',"name":"sachin"},
                        {"email":'enovey@toorakcapital.com',"name":"enovey"},
                        {"email":'lsliwinski@toorakcapital.com',"name":"lsliwinski"},
                        {"email":'LAT@toorakcapital.com',"name":"LAT"},
                        {"email":'afoege@toorakcapital.com',"name":"afoege"},
                        {"email":'skretschmer@toorakcapital.com',"name":"skretschmer"},
                        {"email":'soverton@toorakcapital.com',"name":"sherry"},
                        {"email":'prosalia@toorakcapital.com',"name":"prosalia"},
                        {"email":'kaleemuddin.m@toorakcapital.com',"name":"kaleemuddin"},
                        {"email":'tmargve@toorakcapital.com',"name":"tmargve"},
                        {"email":'cmacintosh@toorakcapital.com',"name":"cmacintosh"},
                        {"email":'jkuppuswamy@toorakcapital.com',"name":"jaya"},
                        {"email":'mcoll@toorakcapital.com',"name":"mcoll"},
                        {"email":'sthegulla@toorakcapital.com',"name":"sthegulla"},
                        {"email":'ragarwal@toorakcapital.com',"name":"ragarwal"},
                        {"email":'sgoldman@toorakcapital.com',"name":"sgoldman"},
                        {"email":'cschollmeyer@toorakcapital.com',"name":"cschollmeyer"},
                        {"email":'vijaylaxmi@triconinfotech.com',"name":"laxmi"},
                        {"email": "mpinnamaneni@toorakcapital.com", "name": "Mounika Pinnamaneni"},
                        {"email":'toorakdatateam@triconinfotech.com',"name":"Data Team"}]
    
    secret_name = {'email_api' : 'dw-noreply-email-api-key',
                   'looker_creds' : 'looker-creds-prod-dw-ue1-sm'}
    
    secret_project_id = 947624841920
