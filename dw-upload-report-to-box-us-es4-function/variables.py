import os
from datetime import datetime
env = os.environ.get('stage', 'dev')
project_id = os.environ.get('project_id', 'toorak-396910')

box_folder_id = '240730218734'
look_id = {'wells_ips' : '250',
            'pst' :{'pst_all_loans' : '251',
                    'pst_summary' : '252',
                    'pst_bridge_summary' : '253',
                    'pst_dscr_summary' : '254'},
            'wells_comparator' : '255'}

wells_box_folder_id = '240730218734'
pst_box_folder_id = '302849922519'

date_columns = ['Breezeway Paid Off Date','Current Maturity Date','Current Maturity Date By Cycle','First Pay Date','Int Accrual Date','Issued Cut Off Date','Last Payment Date','Legal Comments Added Date','Loan Comments Added Date','Maturity Date','Next Payment Date','Note Date','Orig Fico Date','Original Appraisal Date','Original Maturity Date','Property Acquire Dt','Sale Date','Servicer Paid Off Date','Trade Date','Trade Finance Date','Updated Appraisal or BPO Value Date']
dollar_columns = ['Property Homeowner Ins',  'Orig Max Loan',  'UW Net Cash',  'Monthly Tax Amount',  'Updated Appraisal or BPO Value',  'Property Price',  'Original Zillow Median Value',  'Taxes and Insurance',  'Sum of Servicer Upb',  'Total Monthly Gross Rent',  'Principal and Interest',  'Total of Principal Interest Taxes and Insurance',  'Issued Actual TDO Balance',  'Original Principal Balance',  'Monthly Insurance Amount',  'Property Adj Price',  'Draw Escrow',  'Property Tax',  'Cash Out Amt',  'Appraisal Fixed',  'Draw Available Count',  'Trade Wire to Seller',  'Servicer UPB',  'Extension Option Amt',  'Credit Reserve Holdback Dry',  'Current Zillow Median Value',  'Max Loan',  'Appraisal Amt',  'Updated As-Repaired Appraisal or BPO Value',  'Trade Adj Cost Basis Amt',  'Potential Gross Rent',  'UPB Actual Beginning',  'UPB Actual Ending',  'Draw Available',  'Total Draw Amount',  'UnappliedBalance',  'Trade UPB',  'Cost Basis Adjusted Ending',  'Budget']
percentage_columns = ['Percentage of Servicer Upb','Current Loan Rate', 'Credit Reserve Holdback Percent', 'Current Loan Rate By Cycle']

if env == 'prod':
    
    log_bucket_name = f"dw-{env}-cron-job-log-file-execution"
    
    secret_name = {'box_creds' : 'toorak-prod-box-sm-us-ct1',
                  'looker_creds' : 'looker-creds-prod-dw-ue1-sm'}
    
    
    
    secret_project_id = 947624841920
else:
    
    log_bucket_name = f"dw-{env}-cron-job-log-file-execution"
    
    secret_name = {'box_creds' : 'toorak-dev-box-sm-us-ct1',
                   'looker_creds' : 'looker-creds-dev-dw-ue1-sm'}
    
    secret_project_id = 280549663966
