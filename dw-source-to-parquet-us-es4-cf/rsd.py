import re
from variables import *
from main import *

    
def read_excel_rsd(location):
    '''
    '''
    return pd.read_excel(location, dtype=str, skiprows=4)

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
    
def get_folder_name(file_path, folder_regex_pattern):
    '''
    '''
    folderName = re.search(folder_regex_pattern, file_path).group(1)
    folderName = re.sub(r'\s+?\([0-9]+\)', '', folderName)
    return folderName


def trigger_on_rsd_upload(file_path, file_uri):
    folder_regex_pattern = r'([^/]+)\.xlsx$'
    folderName = get_folder_name(file_path, folder_regex_pattern)
    df = read_excel_rsd(file_uri)
    df.drop(df.iloc[:, 0:2], inplace=True, axis=1)
    df = df.filter(regex= '^[#$!@%&\w]')
    df = rename_columns(df)
    # Check if DataFrame is empty
    if df.empty or len(df.columns) == 0:
        response_body = 'File is empty. No further action taken.'
    else:
        ingestion_date = ''.join(file_path.split('/')[-2:-1])

        write_parquet_file(df, folderName,'rsd' ,ingestion_date)
        print('Successfully wrote the RSD Parquet file!')