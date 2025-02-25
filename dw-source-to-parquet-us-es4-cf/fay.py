import re
from variables import *
from main import *
import uuid
from google.cloud import storage
from datetime import datetime

def extract_date(input_string):
    '''
    Extracts a date in the format mm.dd.yyyy and converts it to dd/mm/yy.
    '''
    # Regular expression pattern to match the date
    pattern = r'\(?(\d{1,2})\.(\d{1,2})\.(\d{4})\)?'
    
    # Search for the first occurrence of the date pattern
    match = re.search(pattern, input_string)
    
    if match:
        # Extract day, month, and year
        month, day, year = match.groups()

        # Convert to dd/mm/yy format
        formatted_date = f"{day}/{month}/{year[-2:]}"
        return formatted_date
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

# Mapping of file path patterns to subfolders
FAY_SUBFOLDERS = {
    "Fay_BK": "Fay_BK",
    "Fay_BPO": "Fay_BPO",
    "Fay_Comments": "Fay_Comments",
    "Fay_COVID-19_Pandemic_Assistance": "Fay_COVID-19_Pandemic_Assistance",
    "Fay_EDW_FC": "Fay_EDW_FC",
    "Fay_Escrow": "Fay_Escrow",
    "Fay_FCL": "Fay_FCL",
    "Fay_LossMit": "Fay_LossMit",
    "Fay_Modification": "Fay_Modification",
    "Fay_Standard": "Fay_Standard",
    "Fay_Supplemental": "Fay_Supplemental",
    "Fay_TaxesInsurance": "Fay_TaxesInsurance",  # Default if none match
}

def trigger_on_fay_report(file_path, file_uri):
    """
    Processes Fay report files by extracting date, renaming columns, and saving as Parquet dynamically.
    """
    formatted_date = extract_date(file_path)
    if not formatted_date:
        raise ValueError(f"Could not extract date from file path: {file_path}")

    df = read_csv(file_uri)

    # If the file is empty or has no columns, stop further processing
    if df.empty or len(df.columns) == 0:
        print('File is empty. No further action taken.')
        raise Exception('File is empty. No further action taken.')

    # Rename columns to ensure compatibility
    df = rename_columns(df)

    # Determine the correct subfolder dynamically
    sub_folder = next((folder for folder in FAY_SUBFOLDERS if folder in file_path), "Fay_TaxesInsurance")

    # Write the DataFrame to Parquet
    write_parquet_file(df, "Fay", sub_folder, formatted_date)
    
    print(f"Successfully wrote the Fay {sub_folder} Parquet file!")