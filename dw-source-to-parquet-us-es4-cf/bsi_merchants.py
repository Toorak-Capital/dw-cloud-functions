import re
from variables import *
from main import *

def extract_date(input_string):
    pattern = r'(\d{8})'

    match = re.search(pattern, input_string)
    
    # Check if a match is found
    if match:
        return match.group(0)
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


def trigger_on_bsi_merchants_upload(file_path, file_uri):
        df = read_excel(file_uri)
        df = df.filter(regex= '^[#$!@%&\w]')
        df = rename_columns(df)
        # Check if DataFrame is empty
        if df.empty or len(df.columns) == 0:
            response_body = 'File is empty. No further action taken.'
            status_code = 200
        else:
            ingestion_date = extract_date(file_path)
            formatted_date = datetime.strptime(ingestion_date, "%Y%m%d").date().strftime("%Y-%m-%d")
            write_parquet_file(df, 'Status', 'BSI-Merchants', formatted_date)
            print('Successfully wrote the BSI-Merchants Parquet file!')