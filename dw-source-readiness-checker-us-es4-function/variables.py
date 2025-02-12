import os
from datetime import datetime
env = os.environ.get('stage', 'prod')
project_id = os.environ.get('project_id', 'np-toorak')




monthly_files_bucket_folders_dest_2nd = [
        {'FCI_suspense_balance_report': (f'dw-{env}-bronze-purchased-loan-us-es4-gcs', 'fci/to-process-v2/suspense_balance_report', 7)},
        {'FCI_FPI': (f'dw-{env}-bronze-purchased-loan-us-es4-gcs', 'fci/to-process-v2/fpi', 7)},
        {'FCI_TOS': (f'dw-{env}-bronze-purchased-loan-us-es4-gcs', 'fci/to-process-v2/TOS', 7)},
        {'BSI_ToorakMonthlyAdvanceSummary': (f'dw-{env}-bronze-purchased-loan-us-es4-gcs', 'BSI/to-process-v2/ToorakMonthlyAdvanceSummary', 7)},
        ]


monthly_files_bucket_folders_dest_18th = [
        {'BSI_force_placed_insurance': (f'dw-{env}-bronze-purchased-loan-us-es4-gcs', 'BSI-Merchants/to-process-v2/force_placed_insurance', 18)},
        {'CPUInterestRateTracker': (f'dw-{env}-bronze-purchased-loan-us-es4-gcs', 'PostPurchaseLoanData/to-process-v2/CPUInterestRateTracker', 18)},
        {'toorak_agg': (f'dw-{env}-bronze-purchased-loan-us-es4-gcs', 'Repo/toorak_agg', 18)},
        {'nationstar_monthly_remittance': (f'dw-{env}-bronze-purchased-loan-us-es4-gcs', 'Repo/nationstar_monthly_remittance', 18)}
        ]


rsd_bucket_folder_pairs_dest = [{'RSD_Master Extension Tracker': (f'dw-{env}-bronze-purchased-loan-us-es4-gcs', 'rsd/to-process-v2/Master Extension Tracker', 0)},
        {'RSD_Master Report': (f'dw-{env}-bronze-purchased-loan-us-es4-gcs', 'rsd/to-process-v2/Master Report/', 0)},
        {'RSD_Master RSD Report': (f'dw-{env}-bronze-purchased-loan-us-es4-gcs', 'rsd/to-process-v2/Master RSD Report', 0)},
        {'RSD_Master Special Servicing Weekly': (f'dw-{env}-bronze-purchased-loan-us-es4-gcs', 'rsd/to-process-v2/Master Special Servicing Weekly', 0)}
        ]

bucket_folder_pairs_dest = [
        {'RCN': (f'dw-{env}-bronze-purchased-loan-us-es4-gcs', 'RCN/to-process-v2/DailyTrialBalance', 0)},
        {'BSI_Toorak_LoanAdmin': (f'dw-{env}-bronze-purchased-loan-us-es4-gcs', 'BSI/to-process-v2/Toorak_LoanAdmin', 0)},
        {'BSI_Toorak_PayoffRequest': (f'dw-{env}-bronze-purchased-loan-us-es4-gcs', 'BSI/to-process-v2/Toorak_PayoffRequest', 0)},
        {'BSI_Toorak_BPLSupplementalExtract': (f'dw-{env}-bronze-purchased-loan-us-es4-gcs', 'BSI/to-process-v2/Toorak_BPLSupplementalExtract', 0)},
        {'BSI_Toorak_CashReceipt': (f'dw-{env}-bronze-purchased-loan-us-es4-gcs', 'BSI/to-process-v2/Toorak_CashReceipt', 0)},
        {'BSI_Toorak_LoanDefault': (f'dw-{env}-bronze-purchased-loan-us-es4-gcs', 'BSI/to-process-v2/Toorak_LoanDefault', 0)},
        {'Shellpoint': (f'trk-{env}-sftp-tl-us-es4-gcs', 'Shellpoint', 0)},
        {'FCI_loan_details': (f'dw-{env}-bronze-purchased-loan-us-es4-gcs', 'fci/to-process-v2/loan_details', 0)},
        {'FCI_funding_history_report': (f'dw-{env}-bronze-purchased-loan-us-es4-gcs', 'fci/to-process-v2/funding_history_report', 0)},
        {'FCI_getForeclosure': (f'dw-{env}-bronze-purchased-loan-us-es4-gcs', 'fci/to-process-v2/getForeclosure', 0)},
        {'FCI_getACHStatus': (f'dw-{env}-bronze-purchased-loan-us-es4-gcs', 'fci/to-process-v2/getACHStatus', 0)},
        {'FCI_loan_portfolio_information': (f'dw-{env}-bronze-purchased-loan-us-es4-gcs', 'fci/to-process-v2/loan_portfolio_information', 0)},
        {'BSI-Merchants': (f'dw-{env}-bronze-purchased-loan-us-es4-gcs', 'BSI-Merchants/to-process-v2/Status', 0)},
        {'BZ': (f'dw-{env}-bronze-purchased-loan-us-es4-gcs', 'breezeway/to-process-v2', 0)},
        {'CSL': (f'dw-{env}-bronze-purchased-loan-us-es4-gcs', 'csl/to-process-v2/CSL_Running_Balances', 0)},
        # {'Lending_Home': (f'dw-{env}-bronze-purchased-loan-us-es4-gcs', 'lending-home/to-process-v2/lendinghome', 0)},
        # {'Lima': (f'dw-{env}-bronze-purchased-loan-us-es4-gcs', 'lima/to-process-v2/lima', 0)},
        {'BSI_Toorak_EscrowFPI': (f'dw-{env}-bronze-purchased-loan-us-es4-gcs', 'BSI/to-process-v2/Toorak_EscrowFPI', 0)}
        ]

if env == 'prod':
    Recipients = [
        {"email": "hsharma@toorakcapital.com", "name": "Harsh"},
        {"email": "psahoo@toorakcapital.com", "name": "Premananda Sahoo"},
        {"email": "mpinnamaneni@toorakcapital.com", "name": "Mounika Pinnamaneni"},
        {"email":"vijaylaxmi@triconinfotech.com", "name":"vijaylaxmi"},
        {"email":"sasi.jyothsna@triconinfotech.com", "name":"Sasi Jyothsna"},
        {"email":"ToorakDataTeam@triconinfotech.com", "name":"ToorakData"},
        {"email":"praveenkumar.vs@triconinfotech.com", "name":"Praveen"},
        {"email":"diguvamaheshwarsingh@triconinfotech.com", "name":"mahesh"}
        ]
else:
    Recipients = [
        {"email": "hsharma@toorakcapital.com", "name": "Harsh"},
        {"email": "psahoo@toorakcapital.com", "name": "Premananda Sahoo"},
        {"email": "mpinnamaneni@toorakcapital.com", "name": "Mounika Pinnamaneni"},
        {"email":"vijaylaxmi@triconinfotech.com", "name":"vijaylaxmi"},
        {"email":"sasi.jyothsna@triconinfotech.com", "name":"Sasi Jyothsna"},
        {"email":"ToorakDataTeam@triconinfotech.com", "name":"ToorakData"},
        {"email":"praveenkumar.vs@triconinfotech.com", "name":"Praveen"},
        {"email":"diguvamaheshwarsingh@triconinfotech.com", "name":"mahesh"}
        ]
    
SendEmailCFUrl = f'https://us-east4-{project_id}.cloudfunctions.net/dw-prod-send-email-us-es4-cf'
Subject = 'Alert: Missing Servicer Files'


EmailBody = """<html>
              <head>
                <ul id="ul-img" style="list-style-type: none;margin: 0;padding: 0;background-color: #0fcbef;width: 100%;height: 60px;border-radius: 4px;background-image: -webkit-linear-gradient(97deg, #0fcbef 4%, #1071ee 90%) !important;">
                  <div>
                    <img src="https://file-management-dev-tl-ue1-s3.s3.amazonaws.com/tooraklogo.png" />
                  </div>
                </ul>
            </head>
            <body>
            <br>Hi All,<br><br>This is to inform you that some files are missing for the following servicers:<br>{{html_table}}<br>Please take appropriate action to address this issue.<br><br>Thanks,<br>Toorak.<br>
            <img src='https://static.wixstatic.com/media/74c4f9_20406c39aa61491d83b0ccec086c6feb~mv2_d_4500_4500_s_4_2.png/v1/fit/w_1000%2Ch_1000%2Cal_c/file.png' width="250" height="190">
            </body>
            </html>"""
