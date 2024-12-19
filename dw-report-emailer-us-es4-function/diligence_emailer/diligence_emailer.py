from diligence_emailer.variables import *
from send_email import *
import pandas as pd
import numpy as np
from datetime import datetime
import time
import io
import openpyxl
from google.cloud import storage

file_path = '/tmp/output.xlsx'

def diligence_submission_emailer(file_name, dd_firm, sdk, email_api, bucket, get_bucket):

    
    response = sdk.run_look(str(diligence_submission_id), "csv")
    df = pd.read_csv(io.StringIO(response))
    df.columns = [col if 'Submission Emailer View' not in col else col.replace('Submission Emailer View ','') for col in df.columns]
    
    
    df = df[df['Due Diligence'] == dd_firm]

    if len(df) != 0:

        no_loans = False
        with pd.ExcelWriter(file_path) as writer1:
            df.to_excel(writer1, sheet_name = 'Diligence Submission', index = False)
                
        with open(file_path, 'rb') as f:
            txt=f.read()
            blob = bucket.blob(f'looker_report_emails/{file_name}.xlsx')
            blob.upload_from_string(txt)
            attachments_ = txt
    else:
        no_loans = True
        attachments_ = b""

    if dd_firm == 'Consolidated Analytics':
        recipients_ = ca_email_recipients
        cc_ = ca_email_cc
        
    if dd_firm == 'Opus':
        recipients_ = opus_email_recipients
        cc_ = opus_email_cc
    

    title_ = file_name
    text_ = ''
    if no_loans:
        file_date = file_name.split(' - ')[1]
        print('no loans body')
        body_ = """<html>
              <head>
                <ul id="ul-img" style="list-style-type: none;margin: 0;padding: 0;background-color: #0fcbef;width: 100%;height: 60px;border-radius: 4px;background-image: -webkit-linear-gradient(97deg, #0fcbef 4%, #1071ee 90%) !important;">
                  <div>
                    <img src="https://file-management-dev-tl-ue1-s3.s3.amazonaws.com/tooraklogo.png" />
                  </div>
                </ul>
            </head>
            <body>
            <br>Hi Team,<br><br>No Loans have been sent to dd_firm for todays_date.<br>Please let us know if you face any issues.<br><br>Thanks,<br>Toorak.<br>
            <img src='https://static.wixstatic.com/media/74c4f9_20406c39aa61491d83b0ccec086c6feb~mv2_d_4500_4500_s_4_2.png/v1/fit/w_1000%2Ch_1000%2Cal_c/file.png' width="250" height="190">
            </body>
            </html>"""
        body_ = body_.replace('dd_firm',dd_firm).replace('todays_date',file_date)
    else:
        body_ = """<html>
                <head>
                <ul id="ul-img" style="list-style-type: none;margin: 0;padding: 0;background-color: #0fcbef;width: 100%;height: 60px;border-radius: 4px;background-image: -webkit-linear-gradient(97deg, #0fcbef 4%, #1071ee 90%) !important;">
                    <div>
                    <img src="https://file-management-dev-tl-ue1-s3.s3.amazonaws.com/tooraklogo.png" />
                    </div>
                </ul>
            </head>
            <body>
            <br>Hi All,<br><br>Please find attached report name.<br>Please let us know if you face any issues.<br><br>Thanks,<br>Toorak.<br>
            <img src='https://static.wixstatic.com/media/74c4f9_20406c39aa61491d83b0ccec086c6feb~mv2_d_4500_4500_s_4_2.png/v1/fit/w_1000%2Ch_1000%2Cal_c/file.png' width="250" height="190">
            </body>
            </html>"""
        body_ = body_.replace('report name',file_name)
    
        
    response = send_mail(sender_email, recipients_, cc_, text_, body_, file_name, attachments_, file_name, email_api)
 