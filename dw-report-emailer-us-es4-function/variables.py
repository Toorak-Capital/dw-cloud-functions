import os
from datetime import datetime
env = os.environ.get('stage', 'dev')
project_id = os.environ.get('project_id', 'toorak-396910')

if env == 'prod':
    destination_bucket = f'dw-{env}-bronze-purchased-loan-us-es4-gcs'
    log_bucket_name = f"dw-{env}-cron-job-log-file-execution"
    
    secret_name = {'email_api' : 'dw-noreply-email-api-key',
                   'looker_creds' : 'looker-creds-prod-dw-ue1-sm'}
    
    secret_project_id = 947624841920
else:
    destination_bucket = f'dw-{env}-bronze-purchased-loan-us-es4-gcs'
    log_bucket_name = f"dw-{env}-cron-job-log-file-execution"
    
    secret_name = {'email_api' : 'dw-np-noreply-email-api-key-sm-us-ct1',
                   'looker_creds' : 'looker-creds-dev-dw-ue1-sm'}
    
    secret_project_id = 280549663966
