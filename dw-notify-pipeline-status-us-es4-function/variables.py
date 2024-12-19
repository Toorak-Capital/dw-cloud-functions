import os
from datetime import datetime
env = os.environ.get('stage', 'dev')
project_id = os.environ.get('project_id', 'toorak-396910')
if env == 'prod':
    log_bucket_name = f"dw-{env}-cron-job-log-file-execution"
    topic_path = f'projects/{project_id}/topics/dw-prod-pipeline-us-es4-topic'
    
else:
    log_bucket_name = f"dw-{env}-cron-job-log-file-execution"
    topic_path = f'projects/{project_id}/topics/dw-dev-pipeline-us-es4-topic'