import os

env = os.environ.get('stage', 'prod')
bucket_name = f'dw-{env}-raw-snapshot-us-es4-gcs'
