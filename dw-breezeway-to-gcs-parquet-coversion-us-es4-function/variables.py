import os
from datetime import datetime
env = os.environ.get('stage', 'prod')
if env == 'dev':
    destination_bucket = f'dw-{env}-bronze-purchased-loan-us-es4-gcs'
else:
    destination_bucket = f'dw-{env}-bronze-purchased-loan-us-es4-gcs'
    
formatted_date = datetime.now().strftime("%Y-%m-%d")
data_date_format = datetime.now().strftime("%m/%d/%Y")
folder_regex_pattern = r'dbo/([^/]+)/[^/]+\.csv'
bucket_regex_pattern = r'dw-.*?-gcs'
file_path_regex_pattern = r'/([^/]+/[^/]+/[^/]+\.csv)'