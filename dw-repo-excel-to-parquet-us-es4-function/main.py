import functions_framework
import os
import json
import uuid
from datetime import datetime
import pandas as pd
from variables import *
import re
import google.cloud.logging
from google.cloud import storage
client = google.cloud.logging.Client()
client.setup_logging()

import logging
    
def get_sheet_data(file_path, type, sheet_name='', warehouseline=''):
    '''
    '''
    pattern = r'\d{4}-\d{1,2}-\d{1,2}'
    file_date = match.group(0) if (match := re.search(pattern, file_path)) else None
    print(f"Extracted date: {file_date}")

    df = pd.DataFrame()
    file_path = f'gs://{file_path}'

    sheet_names = pd.ExcelFile(file_path).sheet_names  
    logging.info('sheet_names: %s', sheet_names)

    if type=='tape':
        try:
            df = pd.read_excel(file_path,sheet_name='SHEET_1',dtype=str)
        except ValueError as e:
            df = pd.read_excel(file_path,sheet_name='Sheet1',dtype=str)
    elif type=='consolidated':
        try:
            df = pd.read_excel(file_path,sheet_name='Consolidated',dtype=str)
        except ValueError as e:
            df = pd.read_excel(file_path,sheet_name='Consolidated_Tape',dtype=str)
    elif type=='toorak_agg':
        if not sheet_name:
            df = pd.read_excel(file_path,sheet_name=0,dtype=str)
        else:
            df = pd.read_excel(file_path,sheet_name=sheet_name,dtype=str)
    elif type=='payoff':
        df = get_payoff_df(file_path, sheet_name,warehouseline)
    elif type=='transaction':
        df = pd.read_excel(file_path,sheet_name = 'Transactions',dtype=str)
    # Replace periods (.) with underscores (_) in column names
    df.columns = [col.replace(' ', '') for col in df.columns]
    # Replace periods (.) with underscores (_) in column names
    df.columns = [col.replace('.', '') for col in df.columns]
    # Rename the columns if they start with a number by prefixing with an underscore
    df.columns = ['_' + col if col[0].isdigit() else col for col in df.columns]
    return df,file_date

def get_payoff_df(file_path, sheet_name,warehouseline=''):
    df = pd.DataFrame()
    norm_file_path = file_path.lower()
    # Handles Securitised Loan payoff file (Toorak Daily Payoff Report 2024-1 12-04-2024.xlsx)
    if 'payoff' in norm_file_path and warehouseline=='secur':
        df = pd.read_excel(file_path,dtype=str)
        loan_row_index = df[df.apply(lambda row: row.astype(str).str.contains('Loan Number', case=False, na=False)).any(axis=1)].index
        if loan_row_index.empty:
            return pd.DataFrame()
        table_df = pd.read_excel(file_path, skiprows=loan_row_index[0] + 1)
        table_df = table_df[table_df['Loan Number'].notnull() & table_df['Warehouse Line'].notnull()]
        columns = ['Loan Number','Beginning UPB','Principal','Loss Amount','Gross Interest','Warehouse Line']
        table_df = table_df[columns]
        column_mappings = {
            "Loan Number": "LoanID",
            "Beginning UPB": "BeginningActualUPB",
            "Principal": "PrincipalCollected",
            "Loss Amount": "LossAmount",
            "Gross Interest": "GrossInterestCollected",
            "Warehouse Line": "WarehouseLine"
        }
        table_df.rename(columns=column_mappings, inplace=True)
        return table_df.astype(str)
    # Handles Toorak Equity Payoff Loans(Toorak TCP Equity Payoff Loan Details 12-02-2024.xlsx)
    elif 'payoff' in norm_file_path and warehouseline=='equity':
        return pd.read_excel(file_path,dtype=str,sheet_name='TCP Equity Payoff Loan Details')
    # Handles MS,DB,JPM,Churchill,Bawag  
    elif 'payoff' in norm_file_path and (warehouseline == 'churchill' or warehouseline == 'bawag'):
        return pd.read_excel(file_path,dtype=str,sheet_name=sheet_name)
    elif 'payoff' in norm_file_path and warehouseline!='':
        return pd.read_excel(file_path,dtype=str)
    return df


def write_parquet_file(df, file_date, warehouseline,type):
    parquet_unique_id = 'part-00000-' + str(uuid.uuid4())
    if file_date :
        formatted_date = file_date
    else:
        formatted_date = datetime.today().strftime("%Y-%m-%d")
    if type=='tape':
        file_path = f"gs://{destination_bucket}/Repo/{warehouseline}_toorak_tape/ingestion_date={formatted_date}/{parquet_unique_id}.snappy.parquet"
    elif type=='consolidated':
        file_path = f"gs://{destination_bucket}/Repo/{warehouseline}_toorak_tape_consolidated/ingestion_date={formatted_date}/{parquet_unique_id}.snappy.parquet"
    elif type=='payoff':
        file_path = f"gs://{destination_bucket}/Repo/{warehouseline}_toorak_payoff/ingestion_date={formatted_date}/{parquet_unique_id}.snappy.parquet"
    elif type=='transaction':
        file_path = f"gs://{destination_bucket}/Repo/toorak_transaction_action_detail/ingestion_date={formatted_date}/{parquet_unique_id}.snappy.parquet"
    elif type=='toorak_agg':
        file_path = f"gs://{destination_bucket}/Repo/toorak_agg/ingestion_date={formatted_date}/{parquet_unique_id}.snappy.parquet"
    df.to_parquet(file_path, compression='snappy')

def trigger_on_repo_file_upload(cloudevent, context):
    '''
    '''
    string_data = cloudevent.decode('utf8')
    json_data = json.loads(string_data)
    payload = json_data.get("protoPayload")

    file_path = payload.get('resourceName')
    print(f'file_path={file_path}')

    file_name = file_path.split('/')[-1]
    print(f'file_name={file_name}')

    source_bucket = file_path.split('/')[3]
    print(f'source_bucket={source_bucket}')

    file_path = file_path.replace('projects/_/buckets/','').replace('objects/','')
    file_name = file_name.lower()

    parts = file_path.split("/", 1)  # split into bucket & file path
    bucket_name, obj_file_path = parts[0], parts[1]

    # Initialize GCS client
    storage_client = storage.Client()
    storage_bucket = storage_client.bucket(bucket_name)
    blob = storage_bucket.blob(obj_file_path)

    if blob.exists():
        print(f'{obj_file_path} is already processed')
        return

    status_code = 200
    if '.xlsx' not in file_name:
        response_body = 'not an excel file'
        print(response_body)
        return {
        'statusCode': 200,
        'body': json.dumps(response_body)
    }
    warehouselines = ['ms','db','jpm','bawag','churchill','axos','equity','transaction','toorak_agg','toorak']
    if not any(warehouseline in file_name for warehouseline in warehouselines):
        print(response_body)
        return {
            'statusCode': status_code,
            'body': json.dumps(response_body)
        }

    file_types = ['tape','payoff','transaction','toorak_agg']
    if not any(file_type in file_name for file_type in file_types):
        print(response_body)
        return {
            'statusCode': status_code,
            'body': json.dumps(response_body)
        }

    is_transaction_file = False
    sheet_name =''
    if 'ms' in file_name:
        warehouseline = 'ms'
    elif 'db' in file_name:
        warehouseline = 'db'
    elif 'jpm' in file_name:
        warehouseline = 'jpm'
    elif 'bawag' in file_name:
        warehouseline = 'bawag'
        if 'performing' in file_name:
            sheet_name = 'Bawag Perf Payoff Loan Details'
        elif 'npl' in file_name:
            sheet_name = 'Bawag NPL Payoff Loan Details'
    elif 'churchill' in file_name:
        warehouseline = 'churchill'
        sheet_name = 'Churchill Payoff Loan Details'
    elif 'axos' in file_name:
        warehouseline = 'axos'
    elif 'payoff' in file_name:
        if 'tcp' in file_name and 'equity' in file_name:
            warehouseline = 'equity'
        elif 'daily' in file_name and 'report' in file_name:
            warehouseline = 'secur'
    elif 'transaction' in file_name:
        is_transaction_file = True
    elif 'toorak_agg' in file_name:
        is_monthly_paydown_file = True
    else:
        response_body = 'incorrect format or corrupted data'
        print(response_body)
        return {
        'statusCode': 200,
        'body': json.dumps(response_body)
    }
    status_code = 200
    is_payoff = True if 'payoff' in file_name else False

    response_body = 'Successfully converted to Parquet file!'
    if 'transaction' in file_name:
        transaction_df,file_date = get_sheet_data(file_path,type='transaction')
        if not transaction_df.empty and len(transaction_df.columns) != 0:
            write_parquet_file(transaction_df,file_date,'',type='transaction')
        
    elif 'toorak_agg' in file_name:
        monthly_paydown_df,file_date = get_sheet_data(file_path,type='toorak_agg')
        if not monthly_paydown_df.empty and len(monthly_paydown_df.columns) != 0:
            write_parquet_file(monthly_paydown_df,file_date,warehouseline='',type='toorak_agg')    
    else:
        if not is_payoff:
            tape_df,file_date = get_sheet_data(file_path,type='tape')
            consolidated_df,file_date = get_sheet_data(file_path,type='consolidated')

            if not tape_df.empty and len(tape_df.columns) != 0:
                write_parquet_file(tape_df, file_date,warehouseline,type='tape')
            if not consolidated_df.empty and len(consolidated_df.columns) != 0:
                write_parquet_file(consolidated_df, file_date,warehouseline,type='consolidated')
        else:
            payoff_df,file_date = get_sheet_data(file_path,type='payoff',sheet_name=sheet_name,warehouseline=warehouseline)
            payoff_df['created_at'] = datetime.utcnow()
            if not payoff_df.empty and len(payoff_df.columns) != 0:
                write_parquet_file(payoff_df, file_date,warehouseline,type='payoff')
                
    return {
        'statusCode': status_code,
        'body': json.dumps(response_body)
    }