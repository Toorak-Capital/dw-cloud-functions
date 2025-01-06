import os
from datetime import datetime
env = os.environ.get('stage', 'prod')
project_id = os.environ.get('project_id', 'toorak-396910')
sender_email = {"name":"noreply@toorakcapital.com","email":"noreply@toorakcapital.com"}

if env == 'prod':
    secret_name = {'email_api' : 'dw-noreply-email-api-key'}
    secret_project_id = 280549663966
    
else:
    secret_name = {'email_api' : 'dw-np-noreply-email-api-key-sm-us-ct1'}
    secret_project_id = 947624841920
