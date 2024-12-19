import functions_framework
import os
import json
from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound
import google.auth
from datetime import datetime

from variables import *

# Imports the Cloud Logging client library
import google.cloud.logging
# Instantiates a client
client = google.cloud.logging.Client()
client.setup_logging()
import logging


prod_project_id = "toorak-396910"
np_project_id = "np-toorak"


# Define the schema for the table
schema_count = [
    bigquery.SchemaField("prod_loan_count", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("np_loan_count", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
]

schema_status = [
    bigquery.SchemaField("prod_loan_count", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("np_loan_count", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("prod_status", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("np_status", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
]

schema_servicer = [
    bigquery.SchemaField("prod_loan_count", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("np_loan_count", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("prod_servicer", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("np_servicer", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
]

def create_and_insert(client_np, schema, table_ref, result):
    '''
    '''
    # Check if the table exists
    try:
        # client_np.delete_table(table_ref)
        client_np.get_table(table_ref)
        logging.info(f"Table {table_ref} already exists")
    except NotFound:
        # Table does not exist, create it
        table = bigquery.Table(table_ref, schema=schema)
        table = client_np.create_table(table)  # API request
        logging.info(f"Table {table_ref} created successfully.")

    # Insert row
    logging.info(result)
    client_np.insert_rows_json(table_ref, result)


def get_pipeline_loan_count(client_prod, client_np):
    '''
    '''
    table_ref = f"{np_project_id}.np_prod_promo_check.pipeline_loan_count"

    query = "SELECT COUNT(DISTINCT toorak_loan_id) AS loan_count FROM gold.pipeline_loan"

    results_prod = client_prod.query(query).result()
    results_np = client_np.query(query).result()

    # Process and return results as needed
    result = {'prod_' + k:v for row in results_prod for k,v in dict(row).items()}
    result.update({'np_' + k:v for row in results_np for k,v in dict(row).items()})
    result['updated_at'] = datetime.now().isoformat()

    # Create table and Insert data    
    create_and_insert(client_np, schema_count, table_ref, [result])
    logging.info('Completed : %s', get_pipeline_loan_count)
    

def get_purchased_loan_count(client_prod, client_np):
    '''
    '''
    table_ref = f"{np_project_id}.np_prod_promo_check.purchased_loan_count"

    query = "SELECT COUNT(DISTINCT toorak_loan_id) AS loan_count FROM gold.purchased_loan"
    
    results_prod = client_prod.query(query).result()
    results_np = client_np.query(query).result()

    # Process and return results as needed
    result = {'prod_' + k:v for row in results_prod for k,v in dict(row).items()}
    result.update({'np_' + k:v for row in results_np for k,v in dict(row).items()})
    result['updated_at'] = datetime.now().isoformat()

    # Create table and Insert data    
    create_and_insert(client_np, schema_count, table_ref, [result])
    logging.info('Completed : %s', get_purchased_loan_count)


def get_snapshot_loan_count(client_prod, client_np):
    '''
    '''
    table_ref = f"{np_project_id}.np_prod_promo_check.snapshot_loan_count"

    query = "SELECT COUNT(DISTINCT toorak_loan_id) AS loan_count FROM gold.tc_purchased_loan"
    
    results_prod = client_prod.query(query).result()
    results_np = client_np.query(query).result()

    # Process and return results as needed
    result = {'prod_' + k:v for row in results_prod for k,v in dict(row).items()}
    result.update({'np_' + k:v for row in results_np for k,v in dict(row).items()})
    result['updated_at'] = datetime.now().isoformat()

    # Create table and Insert data    
    create_and_insert(client_np, schema_count, table_ref, [result])
    logging.info('Completed : %s', get_snapshot_loan_count)


def get_pipeline_status_count(client_prod, client_np):
    '''
    '''
    table_ref = f"{np_project_id}.np_prod_promo_check.pipeline_status_count"

    query = """
            SELECT 
                COUNT(DISTINCT toorak_loan_id) AS loan_count,
                loan_state AS status
            FROM gold.pipeline_loan
            GROUP BY loan_state
            ORDER BY loan_state
            """
    
    results_prod = client_prod.query(query).result()
    results_np = client_np.query(query).result()

    # Process and return results as needed
    result = [{'prod_' + k:v for k,v in dict(row).items()} for row in results_prod]
    index = 0
    for row in results_np:
        result[index].update({'np_' + k:v for k,v in dict(row).items()})
        result[index]['updated_at'] = datetime.now().isoformat()
        index += 1
    
    # Create table and Insert data    
    create_and_insert(client_np, schema_status, table_ref, result)
    logging.info('Completed : %s', get_pipeline_status_count)


def get_purchased_status_count(client_prod, client_np):
    '''
    '''
    table_ref = f"{np_project_id}.np_prod_promo_check.purchased_status_count"

    query = """
            SELECT 
                COUNT(DISTINCT toorak_loan_id) AS loan_count,
                loan_status AS status
            FROM gold.purchased_loan
            GROUP BY loan_status
            ORDER BY loan_status
            """
    
    results_prod = client_prod.query(query).result()
    results_np = client_np.query(query).result()

    # Process and return results as needed
    result = [{'prod_' + k:v for k,v in dict(row).items()} for row in results_prod]
    index = 0
    for row in results_np:
        result[index].update({'np_' + k:v for k,v in dict(row).items()})
        result[index]['updated_at'] = datetime.now().isoformat()
        index += 1

    # Create table and Insert data    
    create_and_insert(client_np, schema_status, table_ref, result)
    logging.info('Completed : %s', get_purchased_status_count)


def get_servicer_count(client_prod, client_np):
    '''
    '''
    table_ref = f"{np_project_id}.np_prod_promo_check.servicer_count"

    query = """
            SELECT 
                COUNT(DISTINCT toorak_loan_id) AS loan_count,
                source_servicer AS servicer
            FROM gold.purchased_loan
            GROUP BY source_servicer
            ORDER BY source_servicer
            """
    
    results_prod = client_prod.query(query).result()
    results_np = client_np.query(query).result()

    # Process and return results as needed
    result = [{'prod_' + k:v for k,v in dict(row).items()} for row in results_prod]
    index = 0
    for row in results_np:
        result[index].update({'np_' + k:v for k,v in dict(row).items()})
        result[index]['updated_at'] = datetime.now().isoformat()
        index += 1

    # Create table and Insert data    
    create_and_insert(client_np, schema_servicer, table_ref, result)
    logging.info('Completed : %s', get_servicer_count)


def bigquery_query():
    '''
    '''
    # Load service account JSON keys
    storage_client = storage.Client()
    bucket_path = "controls_cred"
    service_file_prod = "toorak-sa.json"
    service_file_np = "np-toorak-sa.json"

    blob_prod = storage_client.bucket(bucket_name).blob(os.path.join(bucket_path, service_file_prod))
    blob_np = storage_client.bucket(bucket_name).blob(os.path.join(bucket_path, service_file_np))
    
    service_account_info_prod = json.loads(blob_prod.download_as_string())
    service_account_info_np = json.loads(blob_np.download_as_string())

    # with open(service_file_prod, 'r') as f:
    #     service_account_info_prod = json.loads(f.read())

    # with open(service_file_np, 'r') as f:
    #     service_account_info_np = json.loads(f.read())
    
    credentials_prod = service_account.Credentials.from_service_account_info(service_account_info_prod)
    credentials_np = service_account.Credentials.from_service_account_info(service_account_info_np)

    # Initialize BigQuery clients for each project
    client_prod = bigquery.Client(credentials=credentials_prod, project=prod_project_id)
    client_np = bigquery.Client(credentials=credentials_np, project=np_project_id)

    return client_prod, client_np

@functions_framework.http
def run_lambda(request):
    '''
    '''
    client_prod, client_np = bigquery_query()

    get_pipeline_loan_count(client_prod, client_np)

    get_purchased_loan_count(client_prod, client_np)

    get_snapshot_loan_count(client_prod, client_np)

    get_pipeline_status_count(client_prod, client_np)

    get_purchased_status_count(client_prod, client_np)

    get_servicer_count(client_prod, client_np)

    logging.info('Successfully written the output to the bigquery dataset')
    return {
        'statusCode': 200,
        'body': json.dumps('Successfully written the output to the bigquery dataset')
    }
