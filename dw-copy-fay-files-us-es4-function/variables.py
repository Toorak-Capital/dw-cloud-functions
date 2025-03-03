import os

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


