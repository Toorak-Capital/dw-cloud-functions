import re
from variables import *
from main import *

def extract_date(input_string):
    pattern = r'([A-Za-z]{3} \d{4})'

    return match.group(0) if (match := re.search(pattern, input_string)) else None

    
def rename_columns(df):
    try:
        renamed_columns = {}
        for col in df.columns:
            new_col = col
            if col[0].isdigit():
                new_col = f'_{col}'
            if '.' in col:
                new_col = new_col.replace('.', '_')
            renamed_columns[col] = new_col
        return df.rename(columns=renamed_columns)
    except Exception as e:
        print(f"An error occurred while renaming columns: {e}")
        return None


def trigger_on_cpu_interest_rate_report(file_path, file_uri):
        df = read_excel(file_uri)
        df = df.filter(regex= '^[#$!@%&\w]')
        df = rename_columns(df)
        # Check if DataFrame is empty
        if df.empty or len(df.columns) == 0:
            response_body = 'File is empty. No further action taken.'
            status_code = 200
            print(response_body)
        else:
            ingestion_date = extract_date(file_path)
            formatted_date = datetime.strptime(ingestion_date, "%b %Y").strftime("%Y-%m-%d")
            write_parquet_file(df, 'CPUInterestRateTracker', 'PostPurchaseLoanData', formatted_date)
            print('Successfully wrote CPU Interest Rate Tracker Parquet file!')