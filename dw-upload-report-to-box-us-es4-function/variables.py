import os
from datetime import datetime
env = os.environ.get('stage', 'dev')
project_id = os.environ.get('project_id', 'toorak-396910')

box_folder_id = '240730218734'
look_id = {'wells_ips' : '250'}

if env == 'prod':
    
    log_bucket_name = f"dw-{env}-cron-job-log-file-execution"
    
    secret_name = {'box_creds' : '',
                  'looker_creds' : 'looker-creds-prod-dw-ue1-sm'}
    
    
    
    secret_project_id = 947624841920
else:
    
    log_bucket_name = f"dw-{env}-cron-job-log-file-execution"
    
    secret_name = {'box_creds' : 'toorak-dev-box-sm-us-ct1',
                   'looker_creds' : 'looker-creds-dev-dw-ue1-sm'}
    
    secret_project_id = 280549663966
