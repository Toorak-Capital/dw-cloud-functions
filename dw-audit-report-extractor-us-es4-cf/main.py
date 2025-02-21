import os
import io
import looker_sdk
import pandas as pd
from datetime import *
import openpyxl
import json
import requests
import base64
from google.cloud import secretmanager_v1
from variables import *



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



def lambda_handler(response):
    audit_reports()

    return {
        'statusCode': 200,
        'body': json.dumps('Audit Reports uploaded to location successfully')
    }

def audit_reports():

    looker_creds = get_secret(secret_name['looker_creds'])
    os.environ['LOOKERSDK_BASE_URL'] = looker_creds['LOOKERSDK_BASE_URL']
    os.environ['LOOKERSDK_CLIENT_ID'] = looker_creds['LOOKERSDK_CLIENT_ID']
    os.environ['LOOKERSDK_CLIENT_SECRET'] = looker_creds['LOOKERSDK_CLIENT_SECRET']

    
    sdk = looker_sdk.init40()

    for report in look_ids:
        response = sdk.run_look(str(look_ids[report]), "csv")
        result = pd.read_csv(io.StringIO(response), header = None)
        result = result.where(pd.notna(result), None)

        new_header = result.iloc[0] 
        result = result[1:]  
        result.columns = new_header  

        # Reset index if needed
        result.reset_index(drop=True, inplace=True)
        file_path = f'gs://dw-prod-bronze-purchased-loan-us-es4-gcs/audit_report/latest_{report}_report.xlsx'
        if report == 'bridge':
            file_path = f'gs://dw-prod-bronze-purchased-loan-us-es4-gcs/audit_report/latest_{report}_report.xlsx'
            result.rename(columns={'Bridge Audit Report Toorak Loan ID': 'Toorak Loan ID', 'Bridge Audit Report Category': 'Loan Field', 'Bridge Audit Report Loan Info': 'Loan Info', 'Bridge Audit Report Tpr': 'TPR'}, inplace=True)
        else:
            file_path = f'gs://dw-prod-bronze-purchased-loan-us-es4-gcs/audit_report/latest_{report}_report.xlsx'
            result.rename(columns={'Bridge Dscr Report Toorak Loan ID': 'Toorak Loan ID', 'Bridge Dscr Report Loan Field': 'Loan Field', 'Bridge Dscr Report Loan Info': 'Loan Info', 'Bridge Dscr Report Tpr': 'TPR'}, inplace=True)
        with pd.ExcelWriter(file_path) as writer:
            result.to_excel(writer, sheet_name = f'latest_{report}_report')