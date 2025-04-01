import re
from variables import *
from main import *
import zipfile
import re
import pandas as pd
from io import BytesIO
from google.cloud import storage

def extract__date(input_string):
    # Regular expression pattern to match digits
    pattern = r'(\d{8})'
    
    # Search for the first occurrence of digits in the input string
    match = re.search(pattern, input_string)
    
    # Check if a match is found
    if match:
        return match.group(1)
    else:
        return None
    

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
        print(f"An error occurred while renaming columns: {e}")
        return None
    
def get_folder_name(input_string):
    try:        
        # Search for the pattern in the input string
        match = re.search(r'[^/]+/(\d{8})_(.+)\.csv', input_string)
        
        # Check if a match is found
        if match:
            return match.group(2)
        else:
            print("No folder name found in the input")
            return None
    except Exception as e:
        # Handle any errors gracefully
        print(f"An error occurred while extracting folder name: {e}")
        return None

def create_date(f_name):
    return (str(f_name)[2:6])+'/'+(str(f_name)[6:8])+'/'+(str(f_name)[8:10])

def extract_date(file_name):
        return str((re.findall('\d+', file_name )))


def create_folder(name,date):
            return name+'/'+date+'/'

def trigger_on_situs_upload(file_path, bucket_name):
    client = storage.Client()
    blob_name = file_path

    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    print('blob_name:',blob_name)
    
    file_name = blob_name.split("/")[-1]
    f_name = extract_date(file_name)
    date = create_date(f_name)
    
    try:
        pd.to_datetime(date)
    except ValueError:
        return {
            "statusCode": 500,
            "body": "\"The date is not in the correct format\""
        }
    
    if blob_name.endswith('.zip'):
        fol_suffix = 'situs'
        
        folder_name = create_folder(fol_suffix, date)
        
        buffer = BytesIO(blob.download_as_string())
        z = zipfile.ZipFile(buffer)
        
        for file_in_zip in z.namelist():
            with z.open(file_in_zip) as f:
                print('inside')
                date_string = extract__date(file_path)
                formatted_date = datetime.strptime(date_string, "%Y%m%d").date().strftime("%Y-%m-%d")
                print(date_string)
                
                df = pd.read_csv(f, index_col=[0],dtype=str, on_bad_lines='skip')
                df = df.replace('\n|\r', ' ', regex=True)
                output_file_name = file_in_zip.replace("ByClient", "")
                folder_name = get_folder_name(output_file_name)
                write_parquet_file(df, folder_name, 'Situs' ,formatted_date)
        print('Successfully wrote the SITUS Parquet file!')