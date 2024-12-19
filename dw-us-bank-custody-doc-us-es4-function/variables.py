import os

# Imports the Cloud Logging client library
import google.cloud.logging
# Instantiates a client
client = google.cloud.logging.Client()
# Retrieves a Cloud Logging handler based on the environment
# you're running in and integrates the handler with the
# Python logging module. By default this captures all logs
# at INFO level and higher
client.setup_logging()
import logging

env = os.environ.get('stage', 'prod')

destination_bucket = f'dw-{env}-bronze-purchased-loan-us-es4-gcs'
usBankSftpBucketName = f'dw-{env}-us-bank-ftp-us-es4-gcs'
usBankAllBucketName = f'dw-{env}-us-bank-all-files-us-es4-gcs'
