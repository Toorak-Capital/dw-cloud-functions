import os
from datetime import datetime
env = os.environ.get('stage', 'dev')

prefix = 'payment-status-tracker/daily-extracts/'
bookmark_path = 'bookmark/sales_report/pst.csv'

if env == 'dev':
    destination_bucket = f'dw-{env}-bronze-purchased-loan-us-es4-gcs'
    raw_bucket_name = f'dw-{env}-raw-snapshot-us-es4-gcs'
    
else:
    destination_bucket = f'dw-{env}-bronze-purchased-loan-us-es4-gcs'
    raw_bucket_name = f'dw-{env}-raw-snapshot-us-es4-gcs'
