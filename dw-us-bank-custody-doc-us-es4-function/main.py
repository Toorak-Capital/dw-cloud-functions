import functions_framework
import os
import io
import json
import uuid
from datetime import datetime
import pandas as pd
import paramiko
from google.cloud import storage

from variables import *


storage_client = storage.Client()


def get_sftp_conn():
    '''
    '''
    secretKey = os.environ.get('secretKey')
    secretKey = json.loads(secretKey)

    host = secretKey['ub_host']
    port = int(secretKey['ub_port'])
    username = secretKey['ub_username']
    password = secretKey['ub_password']

    #FTP_CONNECTION
    transport = paramiko.Transport((host, port))   
    transport.connect(None, username, password)  
    return paramiko.SFTPClient.from_transport(transport)


def rename_columns(df):
    try:
        renamed_columns = {}
        for col in df.columns:
            new_col = col
            if col[0].isdigit():
                new_col = '_' + col
            if '.' in col:
                new_col = new_col.replace('.', '_')
            renamed_columns[col] = new_col
        return df.rename(columns=renamed_columns)
    except Exception as e:
        logging.error("An error occurred while renaming columns: %s", e)
        return None
    

def write_parquet_file(df, folderName, datetime_str, file_type):
    '''
    '''
    file_date = datetime.strptime(datetime_str, "%Y%m%d%H%M%S")
    ingestion_date = file_date.date()

    df['file_type'] = file_type
    df['file_date'] = file_date

    parquet_unique_id = f"part-00000-{file_type}-{datetime_str}"
    parquet_loc = f"gs://{destination_bucket}/us_bank/{folderName}/ingestion_date={ingestion_date}/{parquet_unique_id}.snappy.parquet"
    logging.info('Parquet file location: %s', parquet_loc)
    
    df.to_parquet(parquet_loc, compression='snappy')


def sftp_to_cloud():
    '''
    '''
    ftp_connection = get_sftp_conn()
    
    #list of files on ftp
    file_list = ftp_connection.listdir_attr(path='Inbox/')
    logging.info('file_list: %s', file_list)

    document_inventory_file_list = []
    
    for file in file_list:
        file_time = datetime.fromtimestamp(file.st_mtime)
        file_name = file.filename

        logging.info('filename: %s', file_name)
        logging.info('file_time: %s', file_time)
            
        with ftp_connection.open(f'/Inbox/{file_name}', "r") as ftp_file:
            #ftp_file.prefetch()
            logging.info("before read: %s", datetime.now())
            ftp_file=ftp_file.read()
            ftp_file = io.BytesIO(ftp_file)
            logging.info("After readin: %s", datetime.now())
        
            bucket2 = storage_client.bucket(usBankAllBucketName)
            blob = bucket2.blob(f'{file_name}')
            blob.upload_from_file(ftp_file)

            logging.info('Uploaded the file: %s', file_name)
    
        if str(file_name).endswith('DOCUMENT_INVENTORY_WITH_EXCEPTIONS_ALL_ACCOUNTS.xlsx'):
            document_inventory_file_list.append(file_name)

    ftp_connection.close()
    return document_inventory_file_list


def excel_to_parquet(document_inventory_file_list):
    '''
    '''
    for file_name in document_inventory_file_list:
        filename_split = file_name.split('.')[-2].split('_')

        df = pd.read_excel(f"gs://{usBankAllBucketName}/{file_name}")
        df = df.filter(regex= '^[#$!@%&\w]')
        df = rename_columns(df)

        datetime_str = filename_split[0]
        file_type = filename_split[1]
        folder_name = '_'.join(filename_split[2:]).lower()

        logging.info('Parquet file loc %s/%s', folder_name, datetime_str)
        write_parquet_file(df, folder_name, datetime_str, file_type)


@functions_framework.http
def sftp_handler(event):
    '''
    '''
    document_inventory_file_list = sftp_to_cloud()

    if not document_inventory_file_list:
        data_format = '%Y' if datetime.now().strftime('%d') == '01' else '%Y%m'
        file_prefix = f'dcs0.toorak.xf00.{datetime.now().strftime(data_format)}'

        blobs = storage_client.list_blobs(usBankAllBucketName, prefix=file_prefix)
        blobs_sorted = sorted(blobs, key=lambda blob: blob.name, reverse=True)
        document_inventory_file_list = [blob.name for blob in blobs_sorted if blob.name.endswith('DOCUMENT_INVENTORY_WITH_EXCEPTIONS_ALL_ACCOUNTS.xlsx')][0:2]
        
    logging.info('document_inventory_file_list: %s', document_inventory_file_list)
    excel_to_parquet(document_inventory_file_list)
    
    return {
        'statusCode': 200,
        'body': json.dumps('successfully executed the job')
    }
