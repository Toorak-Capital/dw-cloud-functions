import re
from variables import *
from main import *


def extract_date(input_string):
    # Regular expression pattern to match digits
    pattern = r"\b(\d{2}) (\d{2}) (\d{4})\.xlsx"
    pattern2 = r"\b(\d{2}).(\d{2}).(\d{4})\.xlsx"
    
    # Search for the first occurrence of digits in the input string
    match = re.search(pattern, input_string)

    match2 = re.search(pattern2, input_string)
    
    # Check if a match is found
    if match:
        month = match.group(1)
        day = match.group(2)
        year = match.group(3)
        
        # Format the date
        formatted_date = f"{year}-{month}-{day}"
        return formatted_date

    elif match2:
        month = match2.group(1)
        day = match2.group(2)
        year = match2.group(3)
        
        # Format the date
        formatted_date = f"{year}-{month}-{day}"
        return formatted_date
    else :
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



def trigger_on_lima_upload(file_path, file_uri):
    df = read_excel(file_uri)
    df = df.filter(regex= '^[#$!@%&\w]')
    df = rename_columns(df)
    # Check if DataFrame is empty
    if df.empty or len(df.columns) == 0:
        print('File is empty. No further action taken.')
    else:
        ingestion_date = extract_date(file_path)
        date_object = datetime.strptime(ingestion_date, "%Y-%m-%d").date().strftime("%m/%d/%Y")
        df['data_date'] = date_object
        print(date_object)
        write_parquet_file(df, 'lima', 'lima' ,ingestion_date)
        print('Successfully wrote the lima Parquet file!')