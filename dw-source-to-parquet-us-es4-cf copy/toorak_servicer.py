import re
from variables import *
from main import *

def extract_date(input_string):
    pattern = r'(\d{2}\.\d{2}\.\d{2})'

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


def trigger_on_toorak_servicer_report(file_path, file_uri):
        data_date_format = datetime.now().strftime("%d/%m/%Y")
        df = read_excel(file_uri, sheet_name='Modifications')
        df = df.filter(regex= '^[#$!@%&\w]')
        df = rename_columns(df)
        # Check if DataFrame is empty
        if df.empty or len(df.columns) == 0:
            response_body = 'File is empty. No further action taken.'
            status_code = 200
            print(response_body)
        else:
            # df['data_date'] = data_date_format
            ingestion_date = extract_date(file_path)
            formatted_date = datetime.strptime(ingestion_date, "%m.%d.%y").date().strftime("%Y-%m-%d")
            write_parquet_file(df, 'ToorakServicerReport', 'PostPurchaseLoanData', formatted_date)
            print('Successfully wrote the Toorak Servicer Parquet file!')