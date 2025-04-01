import re
from variables import *
from main import *

def extract_date(input_string):
    '''
    '''
    # Regular expression pattern to match digits
    pattern = r'\(?(\d{1,2}\.\d{1,2}\.\d{4})\)?'
    
    # Search for the first occurrence of digits in the input string
    match = re.search(pattern, input_string)
    
    # Check if a match is found
    if match:
        split = match.group(1).split('.')

        # Format the date; received as mm-dd-yyyy converted into -> yyyy-mm-dd
        formatted_date = f"{split[2]}-{split[0]}-{split[1]}"
        return formatted_date
    else:
        return None


def trigger_on_am_report(file_path, file_uri):
    '''
    '''
    formatted_date = extract_date(file_path)
    
    df = read_excel(file_uri, sheet_name = 'Toorak Master List_Bridge')
    
    if df.empty or len(df.columns) == 0:
        print('File is empty. No further action taken.')
        raise Exception('File is empty. No further action taken.')
    
    else:
        parent_folder = 'AM-Securitization'
        sub_folder = 'originator_am_mapping'
        write_parquet_file(df, sub_folder, parent_folder, formatted_date)
        print('Successfully wrote the AM Parquet file!')