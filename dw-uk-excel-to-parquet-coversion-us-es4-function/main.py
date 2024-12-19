import functions_framework
import re
import json
import traceback
from io import StringIO
from datetime import datetime, timedelta
import pandas as pd
import requests

# Imports the Cloud Logging client library
import google.cloud.logging
client = google.cloud.logging.Client()
client.setup_logging()
import logging

from variables import *


def get_bucket_name(event):
    '''
    '''
    bucketName = event["resourceName"].split("/")[3]
    return bucketName

def get_key_value(event):
    '''
    '''
    keyValue = "/".join((event["resourceName"].split("/"))[5:])
    return keyValue


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
        logging.error(f"An error occurred while renaming columns: {e}")
        return None


def extract_date(input_string):
    '''
    '''
    # Regular expression pattern to match digits
    pattern = r"([0-9]{2}\s*[A-Z]*[a-z]*\s*[0-9]{4})"
    
    # Search for the first occurrence of digits in the input string
    match = re.search(pattern, input_string)
    
    # Check if a match is found
    if match:
        match = match.group(1)
        year = int(match[-4:])
        date = int(match[:2])
        month = match[2:-4].replace(' ','')
        months = ['january','february','march','april','may','june','july','august','september','october','november','december']
        month = [i for i in months if month.lower() in i][0]
        return datetime.strptime(str(date)+month+str(year),'%d%B%Y')
    else:
        return None


def read_excel(location):
    '''
    '''
    return pd.read_excel(location, dtype=str)


def call_exchange_rate_api():
    '''
    '''
    site_url = 'https://www.federalreserve.gov/releases/h10/hist/dat00_uk.htm'
    response = requests.get(site_url)
 
    if response.status_code == 200:
        return response.text
    else:
        logging.error("Failed to retrieve the webpage. Status code: %s", response.status_code)
        return ''


def call_exchange_rate_api_v6():
    '''
    '''
    site_url = 'https://v6.exchangerate-api.com/v6/d3419e56a2afbbb8f742c2c5/latest/GBP'
    response = requests.get(site_url)
 
    if response.status_code == 200:
        return response.json()
    else:
        logging.error("Failed to retrieve the webpage. Status code: %s", response.status_code)
        return ''


def fetch_exchange_rate():
    '''
    '''
    html_content = call_exchange_rate_api()
    logging.info('html_content: %s', html_content)
    # parse html
    tables = pd.read_html(StringIO(html_content))
    exchange_rate_df = tables[0]
    logging.info('exchange_rate_df: %s', exchange_rate_df)

    exchange_rate_df = exchange_rate_df[exchange_rate_df['Rate'] != 'ND']
    exchange_rate_df['file_date'] =  pd.to_datetime(exchange_rate_df['Date'], format="%d-%b-%y")
    exchange_rate_df['rate'] = exchange_rate_df['Rate'].astype(float)
    exchange_rate_df = exchange_rate_df.drop(columns=['Date', 'Rate'])

    logging.info(exchange_rate_df)
    return exchange_rate_df


def fetch_excel_df(source_bucket, file_path):
    '''
    '''
    df = read_excel(f"gs://{source_bucket}/{file_path}")
    df = df.dropna(axis=1, how='all').dropna(axis=0, how='all')
    df = df.iloc[:,0:4]
    df = df.filter(regex= '^[#$!@%&\w]')
    return df


def fetch_sales_df(raw_df, exchange_rate_df, file_date, previous_sunday_date):
    '''
    '''
    final = []
    categories = []

    sales_list = raw_df.values.tolist()[:-4]
    for i in sales_list:
        categories.append(i[0])

        if i[0] in ['Total', 'Rehab', 'Bridge', 'Ground up', 'Draws', 'Payoffs', 'Total Loans Purchased', 'Total Acquisitions', 'Total Pay Offs', 'No. Current Loans', 'Total AUM']:
            final.append(i)
    
    if 'Ground up' not in categories:
        final.append(['Ground up', 0.0, 0.0, 0.0])

    df = pd.DataFrame(final, columns = ['category', 'week_GBP', 'mtd_GBP', 'eom_GBP'])
    df = df.replace('N/a', 0.0)
    df = df.fillna(0.0)

    df['week_GBP'] = df['week_GBP'].astype(float)
    df['mtd_GBP'] = df['mtd_GBP'].astype(float)
    df['eom_GBP'] = df['eom_GBP'].astype(float)

    df['file_date'] = file_date
    df['date_index_date'] = previous_sunday_date
    df['year'] = previous_sunday_date.year

    df['D_O_Y'] = df['date_index_date'].dt.dayofyear.astype(int)
    df['year'] = df['date_index_date'].dt.year.astype(int)
    # Create date_index column
    df['date_index'] = df['year'].astype(str) + df['D_O_Y'].astype(str)
    df = df.drop_duplicates()

    pd.options.display.float_format = '{:.6f}'.format

    logging.info('merge exchange_rate_df')
    sales_df = pd.merge(df, exchange_rate_df, on='file_date', how='left')
    # GBP to USD conversion
    sales_df['week_USD'] = sales_df['week_GBP'] * sales_df['rate']
    sales_df['mtd_USD'] = sales_df['mtd_GBP'] * sales_df['rate']
    sales_df['eom_USD'] = sales_df['eom_GBP'] * sales_df['rate']

    # Check if the year and month match
    is_current_month = (file_date.year == file_date.year) and (file_date.month == file_date.month)
    if is_current_month and (sales_df['week_USD'].isna()).all():
        result = call_exchange_rate_api_v6()
        sales_df['rate'] = result['conversion_rates']['USD']
        sales_df['week_USD'] = sales_df['week_GBP'] * sales_df['rate']
        sales_df['mtd_USD'] = sales_df['mtd_GBP'] * sales_df['rate']
        sales_df['eom_USD'] = sales_df['eom_GBP'] * sales_df['rate']

    return sales_df


def fetch_wac_df(raw_df, file_date, previous_sunday_date):
    '''
    '''
    wac_list = raw_df.values.tolist()[-4:]
    final = []
    categories = []

    for i in wac_list:
        categories.append(i[0])

        if i[0] in ['Bridging', 'Rehab', 'Ground up']:            
            final.append(i)

    if 'Ground up' not in categories:
        final.append(['Ground up', 0.0, 0.0, 0.0])

    df = pd.DataFrame(final, columns = ['category', 'week_GBP', 'mtd_GBP', 'eom_GBP'])
    df = df.replace('N/a',0.0)
    df = df.fillna(0.0)

    df['week_GBP'] = df['week_GBP'].astype(float)
    df['mtd_GBP'] = df['mtd_GBP'].astype(float)
    df['eom_GBP'] = df['eom_GBP'].astype(float)

    df['file_date'] = file_date
    df['date_index_date'] = previous_sunday_date
    df['year'] = previous_sunday_date.year

    df['D_O_Y'] = df['date_index_date'].dt.dayofyear.astype(int)
    df['year'] = df['date_index_date'].dt.year.astype(int)
    # Create date_index column
    df['date_index'] = df['year'].astype(str) + df['D_O_Y'].astype(str)
    df = df.drop_duplicates()
    return df


def write_parquet_file(df, folderName, file_date):
    '''
    '''
    formatted_date = file_date.date().strftime("%Y-%m-%d")
    parquet_unique_id = 'part-00000-' + str(int(file_date.timestamp()))

    df.to_parquet(f"gs://{destination_bucket}/uk_data/to-process-v2/{folderName}/ingestion_date={formatted_date}/{parquet_unique_id}.snappy.parquet", compression='snappy')

    logging.info("Ingested data into gcs: %s/%s", folderName, formatted_date)


@functions_framework.cloud_event
def trigger_on_uk_upload(cloudevent):
    '''
    '''
    try:
        payload = cloudevent.data.get("protoPayload")    
        event = {"resourceName": payload.get('resourceName')} 
        source_bucket = get_bucket_name(event)
        logging.info("source_bucket: %s", source_bucket)

        file_path = get_key_value(event)
        logging.info("file_path: %s", file_path)

        file_date = extract_date(file_path)
        idx = (file_date.weekday() + 1) % 7 
        previous_sunday_date = (file_date - timedelta(idx))

        df = fetch_excel_df(source_bucket, file_path)
        logging.info("source_df count %s", df.count())

        # Check if DataFrame is empty
        if df.empty or len(df.columns) == 0:
            response_body = 'File is empty. No further action taken.'
            status_code = 500
        else:
            logging.info('caling fetch_exchange_rate')
            exchange_rate_df = fetch_exchange_rate()
            logging.info('caling sales_df')
            sales_df = fetch_sales_df(df, exchange_rate_df, file_date, previous_sunday_date)
            logging.info('caling wac_df')
            wac_df = fetch_wac_df(df, file_date, previous_sunday_date)
        
            logging.info("write data into gcs")
            write_parquet_file(sales_df, 'sales_weekly', file_date)
            write_parquet_file(wac_df, 'wac_weekly', file_date)
            response_body = 'Successfully wrote the Parquet file!'
            status_code = 200    
    except Exception as e:
        response_body = f"Error: {str(e)}"
        status_code = 500
        logging.error('Error : %s', e)
        logging.error(traceback.format_exc())
    
    return {
        'statusCode': status_code,
        'body': json.dumps(response_body)
    }
    