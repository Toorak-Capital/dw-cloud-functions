import functions_framework
import requests
import json
# import boto3
from datetime import datetime
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from google.cloud import storage
from time import sleep
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import os

env = os.environ.get('stage', 'prod')

def read_json_from_gcs():
    # Specify the bucket name and file name
    bucket_name = f'dw-{env}-raw-snapshot-us-es4-gcs'
    file_name = 'fci/daily_extracts/output/output.json'
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    json_content = blob.download_as_string()
    data = json.loads(json_content)
    data = { 'data': [data.get('data')[0],data.get('data')[1]]}
    return data


def helloHttp(request):
    request = read_json_from_gcs()
    request_json = request

    if request_json and 'name' in request_json:
        name = request_json['name']
    response=lambda_handler(request_json)
    return response

def remove_none(l):
    while None in l: l.remove(None)
    return l


def getDateFolderStructure():
    now = datetime.now()
    return str(now.year) + '/' + str(now.month) + '/' + str(now.day) + '/'


Dest_bucket = f"dw-{env}-raw-snapshot-us-es4-gcs"
storage_client = storage.Client()
secretKey = os.environ.get("secretKey")
secretKey = json.loads(secretKey)
fci_header1 = secretKey['fci_header1']
fci_header2 = secretKey['fci_header2']



def file_to_s3(r, output):
    bucket = storage_client.bucket(Dest_bucket)
    try:
        file_name = 'fci/daily_extracts/' + getDateFolderStructure() + r['name'] + '/' + r['name'] + '_' + '.json'
        print(file_name)
        blob = bucket.blob(file_name)
        blob.upload_from_string(json.dumps(output),content_type='application/json')
        print("uploaded succesfully")
    except Exception as e:
        print(e)


def get_data(d, api):
    header2 = {
        f'Authorization' : f'Bearer {fci_header2}'
    }

    input_data = {
        
        "getLenderTrustLedger": {'name': 'getLenderTrustLedger',
                                 'request_body': {
                                     'query': f'{{\n  getLenderTrustLedger(investor:\"INV-1\" dateFrom:\"01/01/2021\"){{\n    type\n    beneficiary\n    dateDeposited\n    reference\n    memo\n    deposit\n    payment\n    balance\n  }}\n}}\n',
                                     'variables': {}}},
        "getPreForeclosure": {'name': 'getPreForeclosure',
                              'request_body': {
                                  'query': f'{{\n    getPreForeclosure{{\n        account\n        loanUid\n        followUpDate\n        lastReview\n        property\n        city\n        state\n        createdAt\n        pfcRequired\n        pfcStart\n        pfcExpires\n        noiSent\n        noiExpires\n        company\n        phone\n        referredToAtty\n        attyNOISent\n        attyReceived\n        attyNOIExpires\n        attyNOIRcvd\n        closed\n        referredToFC\n        resolution\n        pfcOpened\n        pfcStatus\n  }}\n}}\n',
                                  'variables': {}}},
        "getLoanStatusBreakdown": {'name': 'getLoanStatusBreakdown',
                                   'request_body': {
                                       'query': f'{{\n  getLoanStatusBreakdown\n  {{\n    status\n    totalLoans\n    originalBalance\n    principalBalance\n  }}\n}}\n',
                                       'variables': {}}},
        "getFundingInformation": {'name': 'getFundingInformation',
                                  'request_body': {
                                      'query': f'{{\n  getFundingInformation(account:\"{d}\"){{\n    \n    lenderAccount\n    lenderName\n    amountFunded\n    percentageOwned\n    investorRate\n    currentBalance\n    paymentInformation\n    \n  }}\n}}\n',
                                      'variables': {}}},
        "getLoanAttachments": {'name': 'getLoanAttachments',
                               'request_body': {
                                   'query': f'{{\n  getLoanAttachments(account:\"{d}\")\n  {{\n    loanUid\n    account\n    name\n    type\n    date\n  }}\n}}\n',
                                   'variables': {}}},
        "getLoanCharges": {'name': 'getLoanCharges',
                           'request_body': {
                               'query': f'{{\n  getLoanCharges(account:\"{d}\"){{\n    loanAccount\n    date\n    reference\n    description\n    type\n    interestRate\n    interestFrom\n    deferred\n    origianlBalance\n    unpaidBalance\n    accruedInterest\n    totalDue\n    details{{\n      date\n      payerName\n      reference\n      amount\n      prinVendor\n      intVendor\n      prinBehalf\n      intBehalf\n    }}\n  }}\n}}\n',
                               'variables': {}}},
        "getInvoiceDetail": {'name': 'getInvoiceDetail',
                             'request_body': {
                                 'query': f'{{\n  getInvoiceDetail(invoice:\"{d}\"){{\n    fullName\n    dateDue\n    date\n    numInvoice\n    dateReceived\n    isACH\n    account\n    loanAcct\n    borrower\n    propStreet\n    propCity\n    propState\n    propZip\n    loanStatus\n    detailDescription\n    quantity\n    amount\n  }}\n}}\n',
                                 'variables': {}}}

    }
    t2 = time.time()
    request_session = requests.Session()
    request_session.mount('https://', HTTPAdapter(max_retries=3))
    response = request_session.post('https://fapi.myfci.com/graphql', headers=header2, json=input_data[api]['request_body'], timeout=120)
    #print("RESPONSE:",response.text)
    if "error" in response.text:
        print("ERROR:", response.text)
    elif response.text is None:
        return d, None
    print(f"Time for loan {d}:", time.time() - t2)
    try:
        return d, json.loads(response.text)
    except:
        print("ERROR2:", response.text)
        return d, {'data': []}


def parallel_process(data_list, api):
    count = 0
    out = {'data': {api: []}}
    all_tasks = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        for item_index in range(len(data_list)):
            all_tasks.append(
                executor.submit(
                    get_data,
                    data_list[item_index],
                    api
                )
            )
            count = count + 1
        temp_res = {}
        for future in as_completed(all_tasks):
            # print(future.result())
            try:
                loanid, data = future.result()
                if data is None  or  data['data'] == [] or data['data'][api] == [] or data['data'][api] is None:
                    print("Response is NULL")
                    continue
                if isinstance(data['data'][api], list):
                    data['data'][api] = remove_none(data['data'][api])
                    out['data'][api].extend(data['data'][api])
                else:
                    out['data'][api].append(data['data'][api])
            except Exception as e:
                print("ERROR:", data)
                return {
                        "data": data,
                        "error": str(e)
                        }
    print(count)
    return out


def lambda_handler(event):
    data_list = event['data']

    apis = ['getLoanCharges', 'getInvoiceDetail','getLoanAttachments','getLoanStatusBreakdown','getPreForeclosure','getLenderTrustLedger','getFundingInformation']
    for n in range(len(apis)):
        i = apis[n]
        t0 = time.time()
        print("API=", i)
        if i == 'getInvoiceDetail': 
            out = parallel_process(data_list[1], i)
        else: 
            out = parallel_process(data_list[0], i)
        if "error" in out:
            print("ERROR!!!!!!!!!!!")
            return out
        print(out)
        if i == 'getInvoiceDetail': 
            print("number of loans:", len(data_list[1]))
        else: 
            print("number of loans:", len(data_list[0]))
        print(f"TIME FOR {i}:", time.time() - t0)
        file_to_s3({'name': i}, out)

    return {
        'data': data_list[0],
        'statusCode': 200,
        'body': json.dumps('Successful!')
    }





