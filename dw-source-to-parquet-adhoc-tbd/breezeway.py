import re
from variables import *


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


def trigger_on_breezeway_upload(file_path, file_uri):
    formatted_date = datetime.now().strftime("%Y-%m-%d")
    data_date_format = datetime.now().strftime("%m/%d/%Y")
    folder_regex_pattern = r'dbo/([^/]+)/[^/]+\.csv'
    folder_name = get_folder_name(file_path, folder_regex_pattern)
    df = read_csv(file_uri)
    df['data_date'] = data_date_format
    write_parquet_file(df, folder_name, 'breezeway', formatted_date)