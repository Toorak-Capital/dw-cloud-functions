import os
from datetime import datetime
env = os.environ.get('stage', 'prod')
project_id = os.environ.get('project_id', 'toorak-396910')
look_ids = {'bridge': 109,
           'dscr': 110}


if env == 'dev':
    secret_name = {'email_api' : 'dw-np-noreply-email-api-key-sm-us-ct1',
                   'looker_creds' : 'looker-creds-dev-dw-ue1-sm'}
    
    secret_project_id = 280549663966

else:
    secret_name = {'email_api' : 'dw-noreply-email-api-key',
                'looker_creds' : 'looker-creds-prod-dw-ue1-sm'}

    secret_project_id = 947624841920