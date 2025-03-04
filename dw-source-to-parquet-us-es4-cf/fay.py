import re
from variables import *
from main import *
from traceback import format_exc


def extract_date(input_string):
    '''
    Extracts a date in the format mm.dd.yyyy and converts it to dd/mm/yy.
    '''
    # Regular expression pattern to match the date
    pattern = r"(\d{2}\d{2}\d{2})"
    
    # Search for the first occurrence of the date pattern
    match = re.search(pattern, input_string)
    
    # Check if a match is found
    if match:
        date_str = match.group()
        # Convert to a date string
        return datetime.strptime(date_str, '%m%d%y').strftime('%Y-%m-%d')
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
        logging.error("An error occurred while renaming columns: %s", format_exc())
        return None


def trigger_on_fay_report(file_path, file_uri):
    """
    Processes Fay report files by extracting date, renaming columns, and saving as Parquet dynamically.
    """
    formatted_date = extract_date(file_path)
    if not formatted_date:
        logging.error(f"Could not extract date from file path: %s", file_path)
        raise ValueError(f"Could not extract date from file path: {file_path}")

    df = read_csv(file_uri)

    # If the file is empty or has no columns, stop further processing
    if df.empty or len(df.columns) == 0:
        logging.error('File is empty. No further action taken.')
        raise Exception('File is empty. No further action taken.')

    # Rename columns to ensure compatibility
    df = rename_columns(df)

    # Determine the correct subfolder dynamically
    main_folder = 'Fay'
    sub_folder = '_'.join(file_path.split('/')[-1].split('_')[:-1])
    logging.info('main_folder:%s; sub_folder:%s; formatted_date:%s', main_folder, sub_folder, formatted_date)
    
    # Write the DataFrame to Parquet
    write_parquet_by_date(df, main_folder, sub_folder, formatted_date)
    logging.info(f"Successfully wrote the Fay {sub_folder} Parquet file!")
