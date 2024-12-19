from table_funding_emailer.variables import * 
from variables import *
from send_email import *
import io
from PIL import Image
import os
import pandas as pd
import numpy as np
import openpyxl
from openpyxl.utils import get_column_letter
from openpyxl.styles import Alignment
from google.cloud import storage
import json
from openpyxl.styles import Font
import jinja2

file_path = '/tmp/output.xlsx'

def table_funding_report(file_name, sdk, email_api, bucket, get_bucket):

    response = sdk.run_look(str(table_funding_look_ids['loan_info']), "csv") 
    loan_info = pd.read_csv(io.StringIO(response))
    loan_info_cols = ['Spoc','Broker','Toorak Loan ID','Originator Loan ID','Current Status','Date Converted to Initial Review','Date Converted to Review in Progress','Date Converted to Final Review','Date Converted to Approved','Property Address','Property Type','Loan Characterisation','Initial Loan Amount','Max Loan Amount','Inquiries','TC Fails (Y/N)',
'Risk Bucket']
    loan_info.columns = loan_info_cols
    gap = len(loan_info) + 5

    response = sdk.run_look(str(table_funding_look_ids['summary']), "csv")
    summary = pd.read_csv(io.StringIO(response))
    summary_cols = ['Loan Status','Initial Loan Amount','Maximum Loan Amount','Loan Count']
    summary.columns = summary_cols
    print(len(summary))
    
        
    with pd.ExcelWriter(file_path) as writer1:
        loan_info.to_excel(writer1, sheet_name="Loan_Information", startrow=0, startcol=0, index=False)
        summary.to_excel(writer1, sheet_name="Summary", startrow=0, startcol=0, index=False)
            
    with open(file_path, 'rb') as f:
        txt=f.read()
        blob = bucket.blob(f'looker_report_emails/{file_name}.xlsx')
        blob.upload_from_string(txt)
        attachments_ = txt


    title_ = file_name
    text_ = ''
    
    
    recipients_ = table_funding_recipients
    cc_ = table_funding_cc

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
        
    response = send_mail(sender_email, table_funding_recipients, table_funding_cc, text_, body_, file_name, attachments_, file_name, email_api)
 