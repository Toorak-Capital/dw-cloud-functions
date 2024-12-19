import os
from datetime import datetime

env = os.environ.get('stage', 'prod')
destination_bucket = f'dw-{env}-bronze-purchased-loan-us-es4-gcs'
