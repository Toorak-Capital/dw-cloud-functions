import re
from variables import *
from main import *

def extract_date(input_string):
    # Regular expression pattern to match digits
    pattern = r'\d+'
    
    # Search for the first occurrence of digits in the input string
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
    

def get_folder_name(input_string, folder_regex_pattern):
    try:        
        # Search for the pattern in the input string
        match = re.search(folder_regex_pattern, input_string)
        
        # Check if a match is found
        if match:
            return match.group(1)
        else:
            print("No folder name found in the input")
            return None
    except Exception as e:
        # Handle any errors gracefully
        print(f"An error occurred while extracting folder name: {e}")
        return None


def trigger_on_bsi_upload(file_path, file_uri):
    date_string = extract_date(file_path)
    data_date_format = datetime.strptime(date_string, "%Y%m%d").date().strftime("%m/%d/%Y")
    formatted_date = datetime.strptime(date_string, "%Y%m%d").date().strftime("%Y-%m-%d")
    folder_regex_pattern = r'\d{8}_(\w+)\.csv'
    folder_name = get_folder_name(file_path, folder_regex_pattern)
    df = read_csv(file_uri)
    df = rename_columns(df)
    filtered_columns = [col for col in df.columns if not col.startswith('Unnamed')]
    df = df[filtered_columns]
    if df.empty or len(df.columns) == 0:
        print('File is empty. No further action taken.')
        raise Exception('File is empty. No further action taken.')
    elif(len(df.columns) < 4):
        print('File has less columns')
        raise Exception('File has less columns')
    else:
        df['data_date'] = data_date_format
        write_parquet_file(df, folder_name, 'BSI',formatted_date)
        print('Successfully wrote the BSI Parquet file!')