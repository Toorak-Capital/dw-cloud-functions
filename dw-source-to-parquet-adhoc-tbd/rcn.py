import re
from variables import *
from main import *


def extract_date_daily_file(input_string):
    '''
    '''
    # Regular expression pattern to match digits
    pattern = r"(\d{4}_\d{2}_\d{2})"
    
    # Search for the first occurrence of digits in the input string
    match = re.search(pattern, input_string)
    
    # Check if a match is found
    if match:
        return match.group(1).replace('_', '-')
    else:
        return None
    

def extract_date_monthly_file(input_string):
    '''
    '''
    # Regular expression pattern to match digits
    pattern = r"(\d{2}_\d{2}_\d{2})"
    
    # Search for the first occurrence of digits in the input string
    match = re.search(pattern, input_string)
    
    # Check if a match is found
    if match:
        date_str = match.group()
        # Convert to a date string
        return datetime.strptime(date_str, '%m_%d_%y').strftime('%Y-%m-%d')
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


def trigger_on_rcn_upload(file_path, file_uri):
    df = read_excel(file_uri)
    df = df.filter(regex= '^[#$!@%&\w]')
    df = rename_columns(df)
    
    # Check if DataFrame is empty
    if df.empty or len(df.columns) == 0:
        error_message = 'File is empty. No further action taken.'
        print(error_message)
        return {
            'statusCode': 500,
            'body': json.dumps(error_message)
        }
    else:
        if 'monthly_expense' in file_path.lower():
            folder_name = 'Monthly_Expense'
            ingestion_date = extract_date_monthly_file(file_path)
        else:
            folder_name = 'DailyTrialBalance'
            ingestion_date = extract_date_daily_file(file_path)

        write_parquet_file(df, folder_name, 'RCN', ingestion_date)
        print('Successfully wrote the RCN Parquet file!')
    
    return {
        'statusCode': 200,
        'body': json.dumps('Report sent successfully')
    }
