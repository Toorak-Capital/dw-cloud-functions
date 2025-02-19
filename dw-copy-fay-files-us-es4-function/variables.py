import os
from datetime import datetime
from google.cloud import secretmanager_v1
import json
import ast

env = os.environ.get('stage', 'prod')
project_id = os.environ.get('project_id', 'np-toorak')

if env == 'dev':
    gcs_bucket = "trk-dev-sftp-tl-us-es4-gcs"
    fay_sftp_credentials_key = 'dw-np-fay-sftp-sm-us-ct1'
    secret_project_id = 280549663966

else:
    gcs_bucket = "trk-prod-sftp-tl-us-es4-gcs"
    fay_sftp_credentials_key = 'dw-prod-fay-sftp-sm-us-ct1'
    secret_project_id = 947624841920

def get_secret(secret_id):

    client = secretmanager_v1.SecretManagerServiceClient()
    name = f"projects/{secret_project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(name=name)
    secret_data = response.payload.data.decode("UTF-8")
    try:
        secret_dict = json.loads(secret_data)
        return secret_dict
    except json.JSONDecodeError:
        # If it's not JSON, return the data as a plain string
        return secret_data

fay_sftp_credentials = get_secret(fay_sftp_credentials_key)
fay_sftp_credentials = ast.literal_eval(fay_sftp_credentials)
