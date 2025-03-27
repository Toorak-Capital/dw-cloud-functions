import re
from variables import *
from main import *


def extract_date(input_string):
    '''
    '''
    # Regular expression pattern to match digits
    pattern = r'\(?(\d{1,2}\_\d{1,2}\_\d{4})\)?'
    
    # Search for the first occurrence of digits in the input string
    match = re.search(pattern, input_string)
    
    # Check if a match is found
    if match:
        split = match.group(1).split('_')

        # Format the date; received as mm-dd-yyyy converted into -> yyyy-mm-dd
        formatted_date = f"{split[2]}-{split[0]}-{split[1]}"
        return formatted_date
    else:
        return None


def get_folder_name(input_string):
    '''
    '''
    folder_regex_pattern = r'[\d_]+(.+)\.xlsx'
    # Search for the pattern in the input string
    match = re.search(folder_regex_pattern, input_string)
    
    # Check if a match is found
    if match:
        return match.group(1)
    else:
        print("No folder name found in the input")
        return None


def camel_to_snake_clean(col_str):
    '''
    '''
    # Add _ if name begin with digit
    col_str = '_' + col_str if col_str[0].isdigit() else col_str
    # Replace . with _ 
    col_str = col_str.replace('.', '_') if '.' in col_str else col_str

    # Replace special characters with an empty string
    cleaned_str = re.sub(r'[^a-zA-Z0-9]', '', col_str)
    # Convert CamelCase to snake_case
    snake_case_str = re.sub(r'(?<!^)(?=[A-Z])', '_', cleaned_str).lower()
    return snake_case_str


def rename_columns(df):
    '''
    '''
    renamed_columns = {col:camel_to_snake_clean(col) for col in df.columns}
    return df.rename(columns=renamed_columns)


def trigger_on_bsi_monthly_upload(file_path, file_uri):
    '''
    '''
    file_date = datetime.strptime(extract_date(file_path), "%Y-%m-%d").date()
    folder_name = get_folder_name(file_path)

    if file_date and folder_name:
        df = read_excel(file_uri)
        df = df.filter(regex= '^[#$!@%&\w]')
        df = rename_columns(df)

        # Check if DataFrame is empty
        if df.empty or len(df.columns) == 0:
            response_body = 'File is empty. No further action taken.'
        else:
            ingestion_date = str(file_date)
            write_parquet_file(df, folder_name, 'BSI', ingestion_date)
            response_body = 'Successfully wrote the BSI Monthly Parquet file!'
    else:
        response_body = f'file date: {file_date} / folder name: {folder_name} not found.'

    print(response_body)
    return {
            'statusCode': 200,
            'body': json.dumps(response_body)
        }
