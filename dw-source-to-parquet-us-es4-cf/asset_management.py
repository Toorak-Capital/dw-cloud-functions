import re
from variables import *
from main import *

def extract_date(input_string):
    
    
    pattern = r'\(?(\d{1,2}\.\d{1,2}\.\d{4})\)?'
    sec_pattern = r'(\d{2}-[A-Za-z]{3}-\d{4})'

    match = re.search(pattern, input_string)
    sec_match = re.search(sec_pattern, input_string)

    if match:
        split = match.group(1).split('.')
        formatted_date = f"{split[2]}-{split[0]}-{split[1]}"
        return formatted_date
    elif sec_match:
        
        date_obj = datetime.strptime(sec_match, '%d-%b-%Y')
        formatted_date = date_obj.strftime('%Y-%m-%d')
        return formatted_date
    else:
        return None

def write_parquet_file(df, folderName, formatted_date):


    print(folderName)
    if df.empty or len(df.columns) == 0:
        print('File is empty. No further action taken.')
        raise Exception('File is empty. No further action taken.')

    else:
        parquet_unique_id = 'part-00000-' + str(uuid.uuid4())
        df.to_parquet(f"gs://{destination_bucket}/{folderName}/ingestion_date={formatted_date}/{parquet_unique_id}.snappy.parquet", compression='snappy')
        print(f'Successfully wrote the {folderName} file!')


def trigger_on_am_report(file_path, file_uri):
    '''
    '''
    formatted_date = extract_date(file_path)
    
    df = read_excel(file_uri, sheet_name = 'Toorak Master List_Bridge')
    write_parquet_file(df, 'originator_am_mapping', formatted_date)


def trigger_on_securitization_report(file_path, file_uri):

    formatted_date = extract_date(file_path)

    #materially modified loans
    materially_modified_loans = read_excel(file_uri, sheet_name = 'Materially Modified Loans')
    write_parquet_file(materially_modified_loans, 'AM-Securitization/materially_modified_loans', formatted_date)
    
    #exclusion list
    exclusion_list = read_excel(file_uri, sheet_name = 'Exclusion List')

    column_with_data = exclusion_list.iloc[:,0] == 'Originator Loan Number'
    if column_with_data.any():
        
        header_index = column_with_data[column_with_data].index[0]
        start_row = header_index+1
        exclusion_list.columns = exclusion_list.iloc[header_index]
        exclusion_list = exclusion_list.iloc[start_row:].reset_index(drop=True)

    exclusion_list = exclusion_list.dropna(axis = 1, how = 'all')

    exclusion_list_60_deq = exclusion_list.iloc[:,:2].dropna(how = 'all')
    exclusion_list_60_deq = exclusion_list_60_deq[exclusion_list_60_deq['Originator Loan Number'].notna()]
    exclusion_list_default_mortgage_loans = exclusion_list.iloc[:,2:].dropna(how = 'all')
    exclusion_list_default_mortgage_loans = exclusion_list_default_mortgage_loans[exclusion_list_default_mortgage_loans['Originator Loan Number'].notna()]

    write_parquet_file(exclusion_list_60_deq, 'AM-Securitization/exclusion_list/deq_60_plus', formatted_date)
    write_parquet_file(exclusion_list_default_mortgage_loans, 'AM-Securitization/exclusion_list/default_mortgage_loans', formatted_date)


    #funding_account_balance data prep
    funding_account_balance = read_excel(file_uri, sheet_name = 'Funding Account Balance')
    column_with_data = funding_account_balance.iloc[:,0] == 'Securitization Balance'
    if column_with_data.any():
        
        header_index = column_with_data[column_with_data].index[0]
        start_row = header_index+1
        funding_account_balance.columns = funding_account_balance.iloc[header_index]
        funding_account_balance = funding_account_balance.iloc[start_row:].reset_index(drop=True)

    funding_account_balance['Amount'] = funding_account_balance['Amount'].astype(float)

    write_parquet_file(funding_account_balance, 'AM-Securitization/funding_account_balance', formatted_date)


    #targeted rate data
    targeted_rate = read_excel(file_uri, sheet_name = 'Targeted Rate')
    write_parquet_file(targeted_rate, 'AM-Securitization/targeted_rate', formatted_date)

    #trigger rate date
    trigger_rate = pd.read_excel(file_uri, sheet_name='Trigger Data')
    trigger_rate = trigger_rate.rename({'Rev. Per End Date' : 'Rev Per End Date'}, axis = 1)
    write_parquet_file(targeted_rate, 'AM-Securitization/trigger_rate', formatted_date)
