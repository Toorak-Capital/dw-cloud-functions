import os
from datetime import datetime
env = os.environ.get('stage', 'dev')
project_id = os.environ.get('project_id', 'toorak-396910')
sender_email = {"name":"noreply@toorakcapital.com","email":"noreply@toorakcapital.com"}
look_ids = {'dashboard_id': 75}

if env == 'prod':
    log_bucket_name = f"dw-{env}-cron-job-log-file-execution"
    destination_bucket = f'dw-{env}-bronze-purchased-loan-us-es4-gcs'
    email_recipients = [{"email":'jbeacham@toorakcapital.com',"name":"john"},
                        {"email":'svenugopal@toorakcapital.com',"name":"sachin"},
                        {"email":'jkuppuswamy@toorakcapital.com',"name":"jaya"},
                        {"email":'asimanovsky@toorakcapital.com',"name":"alexandra"},
                        {"email":'edyner@toorakcapital.com',"name":"Eric Dyner"},
                        {"email":'jward@toorakcapital.com',"name":"jward"},
                        {"email":'xhuang@toorakcapital.com',"name":"Xander Huang"},
                        {"email":'bd@toorakcapital.com',"name":"Business Development"},
                        {"email":'LAT@toorakcapital.com',"name":"LAT"},
                        {"email":"mbergamaschi@toorakcapital.com", "name":"Mauricio Bergamaschi"},
                        {"email":'wcoffey@toorakcapital.com',"name":"William Coffey"},
                        {"email":'sgoldman@toorakcapital.com',"name":"Scott Goldman"},
                        {"email":'cmacintosh@toorakcapital.com',"name":"Macintosh"},
                        {"email":'lberger@toorakcapital.com',"name":"Lincoln Berger"},
                        {"email":'cschollmeyer@toorakcapital.com',"name":"Caryn Schollmeyer"},
                        {"email":'ToorakDataTeam@triconinfotech.com',"name":"Toorak Data Team"}]
    
    secret_name = {'email_api' : 'dw-noreply-email-api-key',
                   'looker_creds' : 'looker-creds-prod-dw-ue1-sm'}
    
    secret_project_id = 947624841920
    
else:
    log_bucket_name = f"dw-{env}-cron-job-log-file-execution"
    destination_bucket = f'dw-{env}-bronze-purchased-loan-us-es4-gcs'
    email_recipients = [{"email":'vijaylaxmi@triconinfotech.com',"name":"laxmi"},
                        {"email": "mpinnamaneni@toorakcapital.com", "name": "Mounika Pinnamaneni"}]
    
    secret_name = {'email_api' : 'dw-np-noreply-email-api-key-sm-us-ct1',
                   'looker_creds' : 'looker-creds-dev-dw-ue1-sm'}
    
    secret_project_id = 280549663966