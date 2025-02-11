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

env = os.environ.get('stage', 'dev')
project_id = os.environ.get('project_id', 'toorak-396910')

destination_bucket = f'dw-{env}-bronze-purchased-loan-us-es4-gcs'
gcs_prefix_path = "tc_counsel/"
counsel_db = f'{env}_loan_counsel_data'
counsel_collection = 'loancounsels'
