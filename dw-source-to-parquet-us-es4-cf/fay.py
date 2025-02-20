import re
from variables import *
from main import *

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


def trigger_on_fay_report(file_path, file_uri):
    formatted_date = extract_date(file_path)
    
    
    df = read_csv(file_uri)
    
    if df.empty or len(df.columns) == 0:
        print('File is empty. No further action taken.')
        raise Exception('File is empty. No further action taken.')
    
    elif 'Fay/Fay_BK' in file_path:
        parquet_unique_id = 'part-00000-' + str(uuid.uuid4())
        df.to_parquet(f"gs://{destination_bucket}/Fay/Fay_BK/ingestion_date={formatted_date}/{parquet_unique_id}.snappy.parquet", compression='snappy')

    elif 'Fay/Fay_BPO' in file_path:
        parquet_unique_id = 'part-00000-' + str(uuid.uuid4())
        df.to_parquet(f"gs://{destination_bucket}/Fay/Fay_BPO/ingestion_date={formatted_date}/{parquet_unique_id}.snappy.parquet", compression='snappy')

    elif 'Fay/Fay_Comments' in file_path:
        parquet_unique_id = 'part-00000-' + str(uuid.uuid4())
        df.to_parquet(f"gs://{destination_bucket}/Fay/Fay_Comments/ingestion_date={formatted_date}/{parquet_unique_id}.snappy.parquet", compression='snappy')

    elif 'Fay/Fay_COVID-19_Pandemic_Assistance' in file_path:
        parquet_unique_id = 'part-00000-' + str(uuid.uuid4())
        df.to_parquet(f"gs://{destination_bucket}/Fay/Fay_COVID-19_Pandemic_Assistance/ingestion_date={formatted_date}/{parquet_unique_id}.snappy.parquet", compression='snappy')

    elif 'Fay/Fay_EDW_FC' in file_path:
        parquet_unique_id = 'part-00000-' + str(uuid.uuid4())
        df.to_parquet(f"gs://{destination_bucket}/Fay/Fay_EDW_FC/ingestion_date={formatted_date}/{parquet_unique_id}.snappy.parquet", compression='snappy')

    elif 'Fay/Fay_Escrow' in file_path:
        parquet_unique_id = 'part-00000-' + str(uuid.uuid4())
        df.to_parquet(f"gs://{destination_bucket}/Fay/Fay_Escrow/ingestion_date={formatted_date}/{parquet_unique_id}.snappy.parquet", compression='snappy')

    elif 'Fay/Fay_FCL' in file_path:
        parquet_unique_id = 'part-00000-' + str(uuid.uuid4())
        df.to_parquet(f"gs://{destination_bucket}/Fay/Fay_FCL/ingestion_date={formatted_date}/{parquet_unique_id}.snappy.parquet", compression='snappy')

    elif 'Fay/Fay_LossMit' in file_path:
        parquet_unique_id = 'part-00000-' + str(uuid.uuid4())
        df.to_parquet(f"gs://{destination_bucket}/Fay/Fay_LossMit/ingestion_date={formatted_date}/{parquet_unique_id}.snappy.parquet", compression='snappy')

    elif 'Fay/Fay_Modification' in file_path:
        parquet_unique_id = 'part-00000-' + str(uuid.uuid4())
        df.to_parquet(f"gs://{destination_bucket}/Fay/Fay_Modification/ingestion_date={formatted_date}/{parquet_unique_id}.snappy.parquet", compression='snappy')

    elif 'Fay/Fay_Standard' in file_path:
        parquet_unique_id = 'part-00000-' + str(uuid.uuid4())
        df.to_parquet(f"gs://{destination_bucket}/Fay/Fay_Standard/ingestion_date={formatted_date}/{parquet_unique_id}.snappy.parquet", compression='snappy')

    elif 'Fay/Fay_Supplemental' in file_path:
        parquet_unique_id = 'part-00000-' + str(uuid.uuid4())
        df.to_parquet(f"gs://{destination_bucket}/Fay/Fay_Supplemental/ingestion_date={formatted_date}/{parquet_unique_id}.snappy.parquet", compression='snappy')

    else:  # This handles Fay_TaxesInsurance as well as unknown cases
        parquet_unique_id = 'part-00000-' + str(uuid.uuid4())
        df.to_parquet(f"gs://{destination_bucket}/Fay/Fay_TaxesInsurance/ingestion_date={formatted_date}/{parquet_unique_id}.snappy.parquet", compression='snappy')

    print('Successfully wrote the FAY Parquet file!')