import functions_framework
from datetime import *
from variables import *
from breezeway import *
from bsi_merchants import *
from bsi_merchant_weekly import *
from csl import *
from bsi import *
from fci import *
from lending_home import *
from lima import *
from rcn import *
from rsd import *
from shellpoint import *
from situs import *
from bsi_monthly import *
from nation_star import *
from toorak_servicer import *
from cpu_interest_rate import *



def lambda_handler(request, context):
    print(request)
    bucket_name = request['attributes']['bucketId']
    event_type = request['attributes']['eventType']
    file_path = request['attributes']['objectId']
    file_uri = f"gs://{bucket_name}/{file_path}"
    print(file_uri)

    if event_type == 'OBJECT_FINALIZE' and bucket_name == f'dw-{env}-breezewaydb-us-es4-gcs':
        trigger_on_breezeway_upload(file_path, file_uri)
        print("Uploaded breezeway successfully")

    elif event_type == 'OBJECT_FINALIZE' and 'BSI/Daily-BSI/' in file_path:
        trigger_on_bsi_upload(file_path, file_uri)
        print("Uploaded bsi successfully")

    elif event_type == 'OBJECT_FINALIZE' and 'BSI/Monthly-BSI/' in file_path:
        return trigger_on_bsi_monthly_upload(file_path, file_uri)

    elif event_type == 'OBJECT_FINALIZE' and 'BSI/BSI-Merchants/Status_' in file_path:
        trigger_on_bsi_merchants_upload(file_path, file_uri)
        print("Uploaded bsi-merchants successfully")

    elif event_type == 'OBJECT_FINALIZE' and 'BSI-Merchants/reports/' in file_path:
        trigger_on_bsi_merchants_weekly_upload(file_path, file_uri)
        print("Uploaded bsi_merchants_weekly successfully")

    elif event_type == 'OBJECT_FINALIZE' and 'csl/daily-extracts/' in file_path:
        trigger_on_csl_upload(file_path, file_uri)
        print("Uploaded csl successfully")

    elif event_type == 'OBJECT_FINALIZE' and 'fci/daily_extracts/' in file_path:
        trigger_on_fci_upload(file_path, file_uri, bucket_name)
        print("Uploaded fci successfully")
    
    elif event_type == 'OBJECT_FINALIZE' and 'FCI/Monthly-FCI' in file_path and bucket_name == f'trk-{env}-sftp-tl-us-es4-gcs':
        trigger_on_fci_excel_upload(file_uri)
        print("Uploaded fci suspense report successfully")

    elif event_type == 'OBJECT_FINALIZE' and 'lending-home/daily-extracts/' in file_path:
        trigger_on_lending_home_upload(file_path, file_uri)
        print("Uploaded lending-home successfully")

    elif event_type == 'OBJECT_FINALIZE' and 'lima/daily-extracts/' in file_path:
        trigger_on_lima_upload(file_path, file_uri)
        print("Uploaded lima successfully")

    elif event_type == 'OBJECT_FINALIZE' and 'RCN/Daily-RCN/' in file_path:
        trigger_on_rcn_upload(file_path, file_uri)
        print("Uploaded RCN successfully")

    elif event_type == 'OBJECT_FINALIZE' and 'RSD/' in file_path:
        trigger_on_rsd_upload(file_path, file_uri)
        print("Uploaded RSD successfully")

    elif event_type == 'OBJECT_FINALIZE' and 'Shellpoint/' in file_path:
        trigger_on_shellpoint_upload(file_path, file_uri)
        print("Uploaded shellpoint successfully")

    elif event_type == 'OBJECT_FINALIZE' and 'SITUS/' in file_path:
        trigger_on_situs_upload(file_path, bucket_name)
        print("Uploaded shellpoint successfully")

    elif event_type == 'OBJECT_FINALIZE' and 'PostPurchaseLoanData/Nationstar' in file_path:
        trigger_on_nation_star_upload(file_uri)
        print("Uploaded Nationstar successfully")
    
    elif event_type == 'OBJECT_FINALIZE' and 'PostPurchaseLoanData/CPU' in file_path:
        trigger_on_cpu_interest_rate_report(file_path, file_uri)
        print("Uploaded CPU Interest Rate successfully")
        
    elif event_type == 'OBJECT_FINALIZE' and 'PostPurchaseLoanData/Toorak' in file_path:
        trigger_on_toorak_servicer_report(file_path, file_uri)
        print("Uploaded Toorak Servicer successfully")
    else:
        print("Nothing happened")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Report sent successfully')
    }
