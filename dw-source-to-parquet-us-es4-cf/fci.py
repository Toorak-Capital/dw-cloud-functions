import re
from variables import *
from main import *
import json
import uuid
from google.cloud import storage
from datetime import datetime

def extract_date(input_string):
    # Find numbers using regex
    numbers = re.findall(r'\d+', input_string)
    # Convert numbers to integers
    year = int(numbers[0])
    month = int(numbers[1])
    day = int(numbers[2])
    # Convert to date format
    date_str = f"{year:04}-{month:02}-{day:02}"
    date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
    return date_obj

def extract_date_v2(input_string,date_pattern_regex,date_format):
    # Regular expression to match dates in MM.DD.YY format
    match = re.search(date_pattern_regex, input_string)
    if match:
        date_str = match.group()
        # Convert to a datetime object
        date_obj = datetime.strptime(date_str, date_format)
        return date_obj
    else:
        return None

def getDateFolderStructure():
    now = datetime.now()
    return  str(now.year)+'/'+str(now.month)+'/'+str(now.day)+'/'
    


def trigger_on_fci_upload(file_path, file_uri, bucket_name):
    now = datetime.now()
    client = storage.Client()
    date_object = extract_date(file_path)
    data_date_format = date_object.strftime("%m/%d/%Y")
    formatted_date = date_object.strftime("%Y-%m-%d")
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(file_path)
    content = blob.download_as_text()

    json_object = json.loads(content)
    json_data = json_object.get('data')
    if not json_data:
        raise ValueError('Data field not found in JSON')

    api_name = list(json_data.keys())[0]

    if api_name != 'getApiVersion':
        data = json_data.get(api_name)
        cleaned_data = [entry for entry in data if entry is not None]
        if not data:
            raise ValueError(f'No data found for API: {api_name}')

        df = pd.DataFrame(cleaned_data)
        df.replace('\n|\r|\t', ' ', regex=True, inplace=True)
        df['data_date'] = data_date_format
        file_folder = file_path.split('/')[-2]
        parquet_unique_id = f'part-00000-{uuid.uuid4()}'
        parquet_file_path = f"gs://{destination_bucket}/fci/to-process-v2/{file_folder}/ingestion_date={formatted_date}/{parquet_unique_id}.snappy.parquet"
        df.to_parquet(parquet_file_path, compression='snappy')
        print('Successfully wrote the Parquet file!')
        
        csv_buffer = df.to_csv(index=False)
        file_name = file_path.split('/')[-1]
        file_folder = file_path.split('/')[-2]
        date = str(now.year)+'_'+str(now.month)+'_'+str(now.day)+'_'
        file_path = f'fci/csv_converted/{getDateFolderStructure()}{file_folder}/{date}{file_name.split(".")[0]}.csv'
        print(file_path)
        bucket_destination = client.get_bucket(csv_bucket_name_destination)
        blob_destination = bucket_destination.blob(file_path)
        blob_destination.upload_from_string(csv_buffer, content_type='text/csv')
        print('Successfully wrote the fci csv file!')

def trigger_on_fci_excel_upload(file_uri):
    norm_file_uri = file_uri.lower()
    df = read_excel(file_uri)
    df = rename_columns(df)
    # Check if DataFrame is empty
    if df.empty or len(df.columns) == 0:
        return None
    parent_folder = 'fci'
    sub_folder = ''
    date_pattern_regex = ''
    date_format = ''
    if 'fpi' in norm_file_uri:
        sub_folder = 'fpi'
        date_pattern_regex = r'\d{8}'
        date_format = '%Y%m%d'
    elif 'suspense' in norm_file_uri:
        sub_folder = 'suspense_balance_report'
        date_pattern_regex = r'\b\d{2}\.\d{2}\.\d{2}\b'
        date_format = '%m.%d.%y'
        
    ingestion_date = extract_date_v2(file_uri,date_pattern_regex,date_format)
    df['data_date'] = ingestion_date.strftime('%d/%m/%Y')
    df['created_at'] = datetime.utcnow()
    formatted_date = ingestion_date.date()
    write_parquet_file(df, sub_folder, parent_folder , formatted_date)
    print(f'Successfully wrote the fci {sub_folder} Parquet file!')
        