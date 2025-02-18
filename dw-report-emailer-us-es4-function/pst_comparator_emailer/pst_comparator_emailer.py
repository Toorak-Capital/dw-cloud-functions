from pst_comparator_emailer.variables import *
from send_email import *
import pandas as pd
import numpy as np
from datetime import datetime
import time
import io
import openpyxl
from google.cloud import storage

file_path = '/tmp/output.xlsx'

def pst_comparator_emailer(file_name, sdk, email_api, bucket, get_bucket):

        
    recipients_ = pst_comparator_recipients
    cc_ = pst_comparator_cc
    attachments_ = b""
    title_ = file_name
    text_ = ''
    
    body_ = """<html>
        <body>
        <br>Hi All,<br>
        <br>
        Please find below the link to report name.<br>Please let us know if you face any issues.<br>
        <ul>
            <li><a href="https://toorakcapital.app.box.com/s/1po6g384dx1287mgtpjq42vxpaz118zz">Box Folder</a></li>
        </ul>
        
        <br>Thanks,<br>Toorak.<br>
        <img src='https://static.wixstatic.com/media/74c4f9_20406c39aa61491d83b0ccec086c6feb~mv2_d_4500_4500_s_4_2.png/v1/fit/w_1000%2Ch_1000%2Cal_c/file.png' width="250" height="190">
        </body>
        </html>"""
    body_ = body_.replace('report name',file_name)

    response = send_mail(sender_email, recipients_, cc_, text_, body_, file_name, attachments_, file_name, email_api)
