import functions_framework
import json
import uuid
from datetime import datetime
import pandas as pd
from variables import *
import re
import logging
logging.basicConfig(level=logging.DEBUG)

def write_parquet_file(df):
    parquet_unique_id = 'part-00000-' + str(uuid.uuid4())
    formatted_date = datetime.today().strftime("%Y-%m-%d")
    file_path = f"gs://{destination_bucket}/trade_finance_buyer/to-process/ingestion_date={formatted_date}/{parquet_unique_id}.snappy.parquet"
    df.to_parquet(file_path, compression='snappy')

def trigger_on_warehouseline_update(cloudevent, context):
    string_data = cloudevent.decode('utf8')
    json_data = json.loads(string_data)

    payload = json_data.get("protoPayload")
    file_path = payload.get('resourceName')
    print(f'file_path={file_path}')
    file_name = file_path.split('/')[-1]
    print(f'file_name={file_name}')
    source_bucket = file_path.split('/')[3]
    print(f'source_bucket={source_bucket}')

    warehouse_lines = ["Bawag","Churchill","Deutsche Bank-Bridge"]
    found_warehouse_lines = [warehouse_line for warehouse_line in warehouse_lines if warehouse_line in file_name]
    df = pd.read_excel(f'gs://{source_bucket}/{file_name}')

    # Initialize an empty dictionary to store the columns
    columns_to_extract = {}

    # Check for the presence of each column and extract it if it exists
    if 'Loan Number' in df.columns:
        columns_to_extract['loan_number'] = df['Loan Number'].astype(str)
    elif 'Primary Loan ID' in df.columns:
        columns_to_extract['loan_number'] = df['Primary Loan ID'].astype(str)

    if found_warehouse_lines:
        columns_to_extract['warehouse_line'] = found_warehouse_lines[0]
    elif 'Warehouse Line Allocation' in df.columns:
        columns_to_extract['warehouse_line'] = df['Warehouse Line Allocation'].astype(str)

    # If no columns are found, raise an exception
    if not columns_to_extract:
        raise KeyError("None of the expected columns were found in the DataFrame")

    # Combine all extracted columns into a single DataFrame
    extracted_df = pd.DataFrame(columns_to_extract)

    # Use regex to find the date in the format mm.dd.yyyy
    date_pattern = re.compile(r'\d{2}\.\d{2}\.\d{4}')
    match = date_pattern.search(file_name)

    if match:
        # Extract the date string
        date_str = match.group(0)
        
        # Convert to datetime object
        date_obj = datetime.strptime(date_str, '%m.%d.%Y')
        
        # Format date as mm.dd.yyyy
        formatted_date = date_obj.strftime('%m/%d/%Y')

        extracted_df['trade_finance_date'] = formatted_date
    else:
        print("No date found in the filename.")


    if extracted_df.empty or len(df.columns) == 0:
        response_body = 'File is empty. No further action taken.'
        status_code = 200
        raise Exception('File is empty. No further action taken.')
    elif(len(df.columns) < 3):
        raise Exception('File has less columns')
    else:
        write_parquet_file(extracted_df)
        response_body = 'Successfully converted to Parquet file!'
        status_code = 200
    
    return {
        'statusCode': status_code,
        'body': json.dumps(response_body)
    }