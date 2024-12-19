from risk_score_emailer.variables import *
from send_email import *
import pandas as pd
import numpy as np
import time
import io
import openpyxl
from google.cloud import storage

file_path = '/tmp/output.xlsx'



def risk_score_emailer(file_name, frequency, email_body, sdk, email_api, bucket, get_bucket):

    if frequency == 'weekly':
        
        response = sdk.run_look(str(risk_score_look_ids['weekly_report']), "csv")
        df = pd.read_csv(io.StringIO(response))
        df.columns = [col if 'Risk Score Report Weekly' not in col else col.replace('Risk Score Report Weekly ','') for col in df.columns]

    
    if frequency == 'monthly':
        
        response = sdk.run_look(str(risk_score_look_ids['monthly_report']), "csv")
        df = pd.read_csv(io.StringIO(response))
        df.columns = [col if 'Risk Score Report Monthly' not in col else col.replace('Risk Score Report Monthly ','') for col in df.columns]
    

    with pd.ExcelWriter(file_path) as writer1:
        df.to_excel(writer1, sheet_name = 'Risk Score Report', index = False)
            
    with open(file_path, 'rb') as f:
        txt=f.read()
        blob = bucket.blob(f'looker_report_emails/{file_name}.xlsx')
        blob.upload_from_string(txt)
        attachments_ = txt


    title_ = file_name
    text_ = ''
    
    
    recipients_ = risk_score_recipients
    cc_ = risk_score_cc

    body_ = """<html>
              <head>
                <ul id="ul-img" style="list-style-type: none;margin: 0;padding: 0;background-color: #0fcbef;width: 100%;height: 60px;border-radius: 4px;background-image: -webkit-linear-gradient(97deg, #0fcbef 4%, #1071ee 90%) !important;">
                  <div>
                    <img src="https://file-management-dev-tl-ue1-s3.s3.amazonaws.com/tooraklogo.png" />
                  </div>
                </ul>
            </head>
            <body>
            <br>Hi Team,
            <br><br>Please find attached Risk Score Report for the {body_sub} <br>
            Please let us know if you face any issues.
            <br><br>Regards,<br>Toorak.<br>
            <img src='https://static.wixstatic.com/media/74c4f9_20406c39aa61491d83b0ccec086c6feb~mv2_d_4500_4500_s_4_2.png/v1/fit/w_1000%2Ch_1000%2Cal_c/file.png' width="250" height="190">
            </body>
            </html>"""
    body_ = body_.format(body_sub=email_body)
        
    response = send_mail(sender_email, recipients_, cc_, text_, body_, file_name, attachments_, file_name, email_api)
 