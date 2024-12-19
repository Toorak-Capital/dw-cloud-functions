import functions_framework
import json
import uuid
import io
import datetime
from datetime import datetime,timedelta
import pandas as pd
from variables import *
import re
import google.cloud.logging
# Instantiates a client
client = google.cloud.logging.Client()
# Retrieves a Cloud Logging handler based on the environment
# you're running in and integrates the handler with the
# Python logging module. By default this captures all logs
# at INFO level and higher
from google.cloud import storage
storage_client = storage.Client()
client.setup_logging()

import logging

def read_bookmark(destination_bucket,bookmark_path):
    
    bucket = storage_client.bucket(destination_bucket)
    blob = bucket.blob(bookmark_path)
    csv_data = blob.download_as_string()
    is_processed_df = pd.read_csv(io.BytesIO(csv_data))
    return is_processed_df
    
def write_bookmark(is_processed_df,destination_bucket,bookmark_path):
    
    csv_data = is_processed_df.to_csv(index=False)
    bucket = storage_client.get_bucket(destination_bucket)
    blob = bucket.blob(bookmark_path)
    blob.upload_from_string(csv_data)
    
def process_file(file_name,month_end_date,file_date):
    
    bucket = storage_client.bucket(raw_bucket_name)
    blob = bucket.blob(file_name)
    data_bytes = blob.download_as_bytes()
    df = pd.read_excel(io.BytesIO(data_bytes), 'PST')
    print(df.columns)

    if 'MBA-Classification' in df.columns:
        df = df.rename(columns={'MBA-Classification': 'MBA'})

        df['UPB Actual Ending'] = df['UPB Actual Ending'].astype(float)
        df = df[['MBA','UPB Actual Ending']]

        final = df.groupby(['MBA'])['UPB Actual Ending'].sum().reset_index()
        total_upb = sum(final['UPB Actual Ending'])
        final['% of upb'] = (final['UPB Actual Ending']/total_upb)*100 
        final['Product'] = 'Total'
        print(set(final['Product']))
        print(final.columns)

    else:
        print('inside else')
        
        final = df.groupby(['MBA'])['UPB Actual Ending'].sum().reset_index()
        total_upb = sum(final['UPB Actual Ending'])
        final['% of upb'] = (final['UPB Actual Ending']/total_upb)*100 
        upb_sum_60 = final[final['MBA'].isin(['60-89', '90-119', '120+'])]['UPB Actual Ending'].sum()
        upb_sum_REO = final[final['MBA'].isin(['REO'])]['UPB Actual Ending'].sum()
        sum_60 = final[final['MBA'].isin(['60-89', '90-119', '120+'])]['% of upb'].sum()
        sum_reo = sum_60 + final[final['MBA'].isin(['REO'])]['% of upb'].sum()

        new_row = pd.DataFrame([['60+', upb_sum_60, sum_60],['60+ & REO', upb_sum_REO, sum_reo]], columns=['MBA', 'UPB Actual Ending', '% of upb'])
        final = pd.concat([final, new_row], ignore_index=True)
        final['Product'] = 'Total'

        print('done')


        df_dscr = df[df['Product'] == 'DSCR'].groupby(['MBA'])['UPB Actual Ending'].sum().reset_index()
        df_dscr['% of upb'] = (df_dscr['UPB Actual Ending']/sum(df_dscr['UPB Actual Ending']))*100
        upb_sum_60 = df_dscr[df_dscr['MBA'].isin(['60-89', '90-119', '120+'])]['UPB Actual Ending'].sum()
        upb_sum_REO = df_dscr[df_dscr['MBA'].isin(['REO'])]['UPB Actual Ending'].sum()
        sum_60 = df_dscr[df_dscr['MBA'].isin(['60-89', '90-119', '120+'])]['% of upb'].sum()
        sum_reo = sum_60 + df_dscr[df_dscr['MBA'].isin(['REO'])]['% of upb'].sum()

        new_row = pd.DataFrame([['60+', upb_sum_60, sum_60],['60+ & REO', upb_sum_REO, sum_reo]], columns=['MBA', 'UPB Actual Ending', '% of upb'])
        df_dscr = pd.concat([df_dscr, new_row], ignore_index=True)
        df_dscr['Product'] = 'DSCR' 

        print('done')

        df_bridge = df[df['Product'] == 'Bridge'].groupby(['MBA'])['UPB Actual Ending'].sum().reset_index()
        df_bridge['% of upb'] = (df_bridge['UPB Actual Ending']/sum(df_bridge['UPB Actual Ending']))*100
        upb_sum_60 = df_bridge[df_bridge['MBA'].isin(['60-89', '90-119', '120+'])]['UPB Actual Ending'].sum()
        upb_sum_REO = df_bridge[df_bridge['MBA'].isin(['REO'])]['UPB Actual Ending'].sum()
        sum_60 = df_bridge[df_bridge['MBA'].isin(['60-89', '90-119', '120+'])]['% of upb'].sum()
        sum_reo = sum_60 + df_bridge[df_bridge['MBA'].isin(['REO'])]['% of upb'].sum()

        new_row = pd.DataFrame([['60+', upb_sum_60, sum_60],['60+ & REO', upb_sum_REO, sum_reo]], columns=['MBA', 'UPB Actual Ending', '% of upb'])
        df_bridge = pd.concat([df_bridge, new_row], ignore_index=True)
        
        df_bridge['Product'] = 'BRIDGE' 

        print('done')
        
        final = pd.concat([final,df_dscr,df_bridge])

    final['date_'] = month_end_date
    final['file_date'] = file_date

    return final

def get_last_file_of_each_month():
    blobs = storage_client.list_blobs(raw_bucket_name, prefix=prefix)
    dates_list = {}
    for blob in blobs :
        if '.xlsx' in blob.name:
            file_name = blob.name
            print(file_name)
            try:
                parts = file_name.split('/')
                year = int(parts[2])
                month = int(parts[3])
                date = int(parts[4])
                file_date = datetime(year, month, date).date()

                key = f"{year}-{month}"

                if year >= 2022:
                    if key not in dates_list.items():
                        dates_list[key] = file_date
                    elif file_date > dates_list[key]:
                        dates_list[key] = file_date
                
            except:
                pass
    return dates_list


def write_parquet_file(df):
    
    
    df.to_parquet(f"gs://{destination_bucket}/pst_weekly/", partition_cols='file_date')

@functions_framework.cloud_event
def run_pst_monthly(cloudevent):
    '''
    '''
    blobs = storage_client.list_blobs(raw_bucket_name, prefix=prefix)
    pst_weekly = pd.DataFrame()
    current_date = datetime.today()
    current_date_key = f"{current_date.year}-{current_date.month}"
    print(current_date_key)

    dates_list = get_last_file_of_each_month()
    print(dates_list)
    
    is_processed_df = read_bookmark(destination_bucket,bookmark_path)
    print(is_processed_df)
    
    for blob in blobs :
        if '.xlsx' in blob.name:
            file_name = blob.name
            parts = file_name.split('/')
            if parts[2] != 'xlsx' and int(parts[2]) >= 2022:
                try:
                    year = int(parts[2])
                    month = int(parts[3])
                    date = int(parts[4])
                    file_date = datetime(year, month, date).date()
                    key = f"{year}-{month}"
                    print(key)
                    
                    if file_name not in set(is_processed_df['file_name']) and current_date_key != key:
                        print(file_name)
                        print(key)

                        if year >= 2022:                            

            #                 calculate month end date
                            nxt_mnth = file_date.replace(day=28) + timedelta(days=4)
                            month_end_date = nxt_mnth - timedelta(days=nxt_mnth.day)
                            print(nxt_mnth,month_end_date)

                            

                            if file_date == dates_list[key]:

                                print('file date match')
                                df = process_file(file_name,month_end_date,file_date)
                                print('process done')
                                pst_weekly = pd.concat([pst_weekly,df], ignore_index= True)
                            
                            new_row = pd.DataFrame([[file_name,True]], columns=['file_name', 'is_processed'])
                            is_processed_df = pd.concat([is_processed_df, new_row], ignore_index=True)
                    else:
                        pass

                except Exception as e:
                    print(str(e))
    if not pst_weekly.empty:
        write_parquet_file(pst_weekly)
        write_bookmark(is_processed_df,destination_bucket,bookmark_path)
    response_body = 'Successfully wrote the Parquet file!'
    status_code = 200

        
    return {
        'statusCode': status_code,
        'body': json.dumps(response_body)
    }
    