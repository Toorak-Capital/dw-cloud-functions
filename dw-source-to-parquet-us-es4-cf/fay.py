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
}

def trigger_on_fay_report(file_path, file_uri):
    formatted_date = extract_date(file_path)
    if not formatted_date:
        raise ValueError(f"Could not extract date from file path: {file_path}")

    df = read_csv(file_uri)

    # If the file is empty or has no columns, stop further processing
    if df.empty or len(df.columns) == 0:
        print('File is empty. No further action taken.')
        raise Exception('File is empty. No further action taken.')

    # Determine the subfolder dynamically from the file path
    sub_folders = [
        "Fay_BK", "Fay_BPO", "Fay_Comments", "Fay_COVID-19_Pandemic_Assistance",
        "Fay_EDW_FC", "Fay_Escrow", "Fay_FCL", "Fay_LossMit", "Fay_Modification",
        "Fay_Standard", "Fay_Supplemental", "Fay_TaxesInsurance"
    ]

    # Check which subfolder the file belongs to
    sub_folder = next((folder for folder in sub_folders if folder in file_path), "Fay_TaxesInsurance")

    # Call write_parquet_file function
    write_parquet_file(df, "Fay", sub_folder, formatted_date)
    
    print(f"Successfully wrote the Fay {sub_folder} Parquet file!")