import re
from variables import *
from main import *

def extract_date(input_string):
    pattern = r'\(?(\d{1,2}\.\d{1,2}\.\d{4})\)?'
    match = re.search(pattern, input_string)

    if match:
        # Extract the date string
        date_str = match.group(1)
        
        date_obj = datetime.strptime(date_str, '%m.%d.%Y')
        formatted_date = date_obj.strftime('%Y-%m-%d')
        
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

def get_force_placed_insurance_df(file_uri):  # sourcery skip: extract-method
    sheet_name = 'Force Placed'
    
    try:
        # Read the Excel file and find the row index containing "Loan #"
        df = pd.read_excel(file_uri, sheet_name)
        loan_row_index = df[df.apply(lambda row: row.astype(str).str.contains('Loan #', case=False, na=False)).any(axis=1)].index
        
        if loan_row_index.empty:
            return pd.DataFrame()

        # Skip the rows above the "Loan #" row and read the relevant data
        table_df = pd.read_excel(file_uri, sheet_name, skiprows=loan_row_index[0] + 1)

        # Standardize the column names
        table_df.columns = (
            table_df.columns
            .str.replace('#', '', regex=False)
            .str.strip()
            .str.replace('\n', '_', regex=False)
            .str.replace(' ', '_', regex=False)
            .str.lower()
        )

        # Filter out rows with null values in 'loan' and 'trans_code'
        return table_df[table_df['loan'].notnull() & table_df['trans_code'].notnull()]

    except Exception as e:
        print(f"Error processing the file: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on error

def trigger_on_bsi_merchants_weekly_upload(file_path, file_uri):
    if 'berkshire' in file_uri.lower() and 'premium' in file_uri.lower():
        df = get_force_placed_insurance_df(file_uri)
        df['data_date'] = datetime.today().strftime('%d/%m/%Y')
        formatted_date = datetime.today().strftime('%Y-%m-%d')
        write_parquet_file(df, 'force_placed_insurance', 'BSI-Merchants' , formatted_date)
        print('Successfully wrote force_placed_insurance Parquet file!')
    else:
        df = read_excel(file_uri)
        df = df.filter(regex= '^[#$!@%&\w]')
        df = rename_columns(df)
        # Check if DataFrame is empty
        if df.empty or len(df.columns) == 0:
            response_body = 'File is empty. No further action taken.'
            status_code = 200
        else:
            ingestion_date = extract_date(file_path)
            df['data_date'] = datetime.strptime(ingestion_date, '%Y-%m-%d').strftime('%d/%m/%Y')
            formatted_date = ingestion_date
            write_parquet_file(df, 'YTD-Production', 'BSI-Merchants' , formatted_date)
            print('Successfully wrote the BSI-Merchants Weekly Parquet file!')