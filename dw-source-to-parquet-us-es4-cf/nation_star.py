import re
from variables import *
from main import *

def extract_date_v2(input_string):
    # Regular expression to match dates in MM.DD.YY format
    date_pattern = r'\b\d{2}\.\d{2}\.\d{4}\b'
    match = re.search(date_pattern, input_string)
    if match:
        date_str = match.group()
        # Convert to a datetime object
        date_obj = datetime.strptime(date_str, '%m.%d.%Y')
        return date_obj
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

def trigger_on_nation_star_upload(file_uri):
    '''
    '''
    normalised_file_url = file_uri.lower()
    if 'nationstar' not in normalised_file_url and 'remittance' not in normalised_file_url:
        return None
    df = pd.read_excel(file_uri,dtype=str, sheet_name='LOAN_LEVEL',skiprows=0)
    std_df = rename_columns(df)
    
    # Check if DataFrame is empty
    if std_df.empty or len(std_df.columns) == 0:
        return None
    ingestion_date = extract_date_v2(file_uri)
    
    formatted_date = ingestion_date.strftime('%Y-%m-%d')
    parent_folder = 'Repo'
    sub_folder = 'nationstar_monthly_remittance'
    write_parquet_file(std_df, sub_folder, parent_folder, formatted_date)
    
    print(f'Successfully wrote the {sub_folder} Parquet file!')