import functions_framework
import os
import json
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import storage



env = os.environ.get('stage', 'prod')

@functions_framework.http
def hello_http(request):
    response=lambda_handler("","")
   
    return response

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
    '''
    '''
    bucket = storage_client.bucket(Dest_bucket)
    try:
        file_name = 'fci/daily_extracts/' + getDateFolderStructure() + r['name'] + '/' + r['name'] + '_' + '.json'
        blob = bucket.blob(file_name)
        blob.upload_from_string(json.dumps(output),content_type='application/json')
    except Exception as e:
        print(e)


def get_data(d):

    header2 = {
        f'Authorization' : f'Bearer {fci_header2}'
    }

    input_data = {
        'query': f'{{\n  getLoanDetails(account:\"{d}\"){{\n    account\n    aCHStatus\n    aCHStatusEnum\n    aRMName\n    aRMOptionActive\n    borrowerEmail\n    borrowerFax\n    borrowerFullName\n    borrowerHomePhone\n    borrowerMailingAddress\n    borrowerMobilePhone\n    borrowerTIN\n    borrowerTINMask\n    borrowerTINParse\n    borrowerTINType\n    borrowerWorkPhone\n    borrowerZip\n    coBorrower\n    deferredUnpaidLateCharges\n    deferredPrinBal\n    deferredUnpaidInt\n    fciServiceProgram\n    escrowBalance\n    floatCapForNegAmort\n    floatCapForPayment\n    floatCeiling\n    floatDaysAfterPymtChange\n    floatDaysAfterRateChange\n    floatEnabledPymtAdj\n    floatEnableFirstRateCap\n    floatEnableRecast\n    floatEnableSurplus\n    floatFirstRateMaxCap\n    floatFirstRateMinCap\n    floatFloor\n    floatFreqPymtChange\n    floatFreqRateChange\n    floatFreqRecast\n    floatIndex\n    floatLastRecast\n    floatMargin\n    floatNextAdjPayment\n    floatPeriodicMaxCap\n    floatPeriodicMaxCap\n    floatRoundMethod\n    floatRoundRateFactor\n    floatSendNotice\n    floatStopRecast\n    floatSurplus\n    isOnHold\n    graceDays\n    minimunLateCharges\n    lateChargesPct\n    lienPosition\n    loanMaturity\n    nextPaymentDue\n    noteRate\n    noteType\n    noteTypeEnum\n    originalLoanAmount\n    loanOrigination\n    loanPayoff\n    paidToDate\n    payment\n    escrowPayment\n    paymentOthers\n    suspensePayment\n    currentLoanAmount\n    propertyCity\n    propertyState\n    propertyStreet\n    propertyZip\n    rateType\n    rateTypeEnum\n    suspenseBalance\n    restrictedSuspense\n    investorRate\n    status\n    statusEnum\n    unearnedDiscount\n    unpaidInterest\n    unpaidLateCharges\n    defaultRate\n    accruedLateCharges\n    loanCharges\n    deferredUnpaidLoanCharges\n    seniorloanAmounts\n    totalPayment\n    drawStatus\n    closeDate\n    maximumDraw\n    fundedAmount\n    avaliableBalance\n    uRLEppraisal\n    uRLForeclosures\n    uRLGoogle\n    uRLListings\n  }}\n}}\n',
        'variables': {}}

    request_session = requests.Session()
    request_session.mount('https://', HTTPAdapter(max_retries=3))
    response = request_session.post('https://fapi.myfci.com/graphql', headers=header2, json=input_data, timeout=120)
  
    return d, json.loads(response.text)


def lambda_handler(event, context):
  
    # Daily Distribution Reports - API
    headers = {
        f'Authorization' : f'Bearer {fci_header1}'
    }

    # FCI Web Loan Information - API
    header2 = {
        f'Authorization' : f'Bearer {fci_header2}'
    }

    api_list = [
                {'name': 'funding_history_report',
                 'request_body': {
                     'query': '{\n  getFundingHistory{\n    loanAccount\n    amortizationType\n    appraiserDate\n    appraiserMarketValue\n    armOptionActive\n    article7\n    assignment\n    boardingDate\n    borrowerAddress\n    borrowerCity\n    borrowerEmail\n    borrowerFax\n    borrowerFirstName\n    borrowerFullName\n    borrowerHomePhone\n    borrowerLastName\n    borrowerMI\n    borrowerMobilePhone\n    borrowerState\n    borrowerWorkPhone\n    borrowerZip\n    brokerRepresentative\n    chargesAdjustment\n    cumulativeDraw\n    deferredLateCharges\n    deferredPrinBal\n    deferredUnpaidCharges\n    deferredUnpaidInt\n    depositAmount\n    depositDate\n    depositFee\n    depositNotes\n    depositReference\n    description\n    draws\n    ficoScore\n    firstPaymentDate\n    floatCapForNegAmort\n    floatCapForPayment\n    floatCeiling\n    floatDaysAfterPymtChange\n    floatDaysAfterRateChange\n    floatEnabledPymtAdj\n    floatEnableFirstRateCap\n    floatEnableLastRecast\n    floatEnableRecast\n    floatFirstRateMaxCap\n    floatFirstRateMinCap\n    floatFloor\n    floatFreqPymtChange\n    floatFreqRateChange\n    floatFreqRecast\n    floatIndex\n    floatLastRecast\n    floatMargin\n    floatNextAdjPayment\n    floatNextAdjRate\n    floatNextAdjRecast\n    floatPeriodicMaxCap\n    floatPeriodicMinCap\n    floatRoundMethod\n    floatRoundRateFactor\n    floatSendNotice\n    floatStopRecast\n    funds\n    impoundBalance\n    lateChargesDays\n    lateChargesMin\n    lateChargesPct\n    lenderAccount\n    lenderFullName\n    lienPosition\n    loanChargesAccruedInterest\n    loanChargesPrincipal\n    maturityDate\n    nextDueDate\n    noteRate\n    noteType\n    occupancyStatus\n    originalBalance\n    originationDate\n    paidOffDate\n    paidToDate\n    payment\n    paymentImpound\n    paymentReserve\n    prevAccount\n    principalBalance\n    principalWaived\n    propertyAPN\n    propertyCity\n    propertyState\n    propertyStreet\n    propertytype\n    propertyZip\n    purpose\n    rateType\n    reserveBalance\n    section32\n    seniorLoanAmount\n    status\n    thomasMap\n    unearnedDiscount\n    unpaidCharges\n    unpaidInterest\n    unpaidInterestWaived\n    unpaidLateCharges\n    unpaidLateChargesWaived\n    unpaidLateChargesWaived\n  }\n}',
                     'variables': {}}},

                {'name': 'interest_accrual_report',
                 'request_body': {
                     'query': '{\n  getInterestAccrual\n  {\n    loanAccount\n    accrualMethod\n    accruedInterestMTD\n    currentMonth\n    dailyRateUsing\n    daysBetweenDates\n    includEcalcInt\n    lenderAccount\n    loanNextDueDate\n    negativeAmortization\n    noteRate\n    paidToDate\n    payAutomatically\n    principalBalance\n  }\n}',
                     'variables': {}}},
                
                {'name': 'loan_activities_report',
                 'request_body': {
                     'query': '{\n  getLoanActivities{\n    loanAccount\n    achDate\n    achTransNumber\n    balance\n    clearingDate\n    dateDeposited\n    dateDue\n    dateReceived\n    dayVariance\n    defaultIntEffectiveDateFrom\n    defaultIntImplementationDate\n    description\n    interestPaidTo\n    lateCharge\n    lenderAccount\n    notes\n    reference\n    releaseDate\n    releaseDateExt\n    reserveRestricted\n    revDateDue\n    revInterestPaidTo\n    revPaidOff\n    toAdvanceRentReserve\n    toBrokerFee\n    toCapitalExp\n    toChargesInterest\n    toChargesPrincipal\n    toExpenseReserve\n    toImpound\n    toImpoundEstimated\n    toInsuranceAdvanceReserve\n    toInsuranceReserve\n    toInterest\n    toInterestEstimated\n    toLateCharge\n    toLenderFee\n    toMiscellaneous\n    toOtherPayments\n    toOtherTaxable\n    toOtherTaxFree\n    toPrepay\n    toPrincipal\n    toPrincipalEstimated\n    toPropertyManagement\n    toRepair\n    toReserve\n    toSecurityDeposit\n    totalEstimated\n    toTaxAdvanceReserve\n    toTaxReserve\n    toUnpaidDefaultInt\n    toUnpaidEscrowInt\n    toUnpaidFees\n    toUnpaidInterest\n    totalPayment\n    pppPayment\n  }\n}',
                     'variables': {}}},
                
                {'name': 'loan_charges_report',
                 'request_body': {
                     'query': '{\n  getLoanCharges{\n    loanAccount\n    date\n    reference\n    description\n    type\n    interestRate\n    interestFrom\n    deferred\n    origianlBalance\n    unpaidBalance\n    accruedInterest\n    totalDue\n    details{\n      date\n      payerName\n      reference\n      amount\n      prinVendor\n      intVendor\n      prinBehalf\n      intBehalf\n    }\n  }\n}',
                     'variables': {}}},

                {'name': 'lender_payment_statement_history',
                 'request_body': {
                     'query': '{\n  getLenderPaymentStatementHistory{\n    loanAccount\n    checkAmount\n    checkDate\n    checkNo\n    code\n    interest\n    interestCharges\n    lateCharges\n    lenderAccount\n    name\n    opmAmount\n    otherNonTaxable\n    otherPayments\n    otherTaxable\n    pmtDueDate\n    prepayFee\n    principal\n    principalCharges\n    serviceFee\n  }\n}',
                     'variables': {}}},

                {'name': 'other_payments_report',
                 'request_body': {
                     'query': '{\n  getOtherPayments{\n    loanAccount\n    amount\n    checkDate\n    checkNo\n    code\n    description\n    lenderAccount\n    name\n    subType\n    toOtherPayments\n    toOtherTaxable\n    toOtherTaxFree\n  }\n}',
                     'variables': {}}},

                {'name': 'loan_payString_Report',
                 'request_body': {
                     'query': '{\n  getLoanPayString{\n    borrowerFullName\n    currentDQStatus\n    firstPaymentDate\n    lateChargesDays\n    lenderAccount\n    loanAccount\n    nextDueDate\n    payString\n    principalBalance\n  }\n}',
                     'variables': {}}},

                {'name': 'loan_portfolio_information',
                 'request_body': {
                     'query': '{ \n  getLoanInformation\n{\n    loanAccount\n    achStatus\n    amortizationType\n    appraiserDate\n    appraiserMarketValue\n    appTimeStamp\n    aRMOptionActive\n    article7\n    assignment\n    boardingDate\n    borrowerAddress\n    borrowerCity\n    borrowerEmail\n    borrowerFax\n    borrowerFirstName\n    borrowerFullName\n    borrowerHomePhone\n    borrowerLastName\n    borrowerMI\n    borrowerMobilePhone\n    borrowerState\n    borrowerWorkPhone\n    borrowerZip\n    chargesAdjustment\n    deferredLateCharges\n    deferredPrinBal\n    deferredUnpaidCharges\n    deferredUnpaidInt\n    draws\n    ficoScore\n    firstPaymentDate\n    floatCapForNegAmort\n    floatCapForPayment\n    floatCeiling\n    floatDaysAfterPymtChange\n    floatDaysAfterRateChange\n    floatEnabledPymtAdj\n    floatEnableFirstRateCap\n    floatEnableLastRecast\n    floatEnableRecast\n    floatFirstRateMaxCap\n    floatFirstRateMinCap\n    floatFloor\n    floatFreqPymtChange\n    floatFreqRateChange\n    floatFreqRecast\n    floatIndex\n    floatLastRecast\n    floatMargin\n    floatNextAdjPayment\n    floatNextAdjRate\n    floatNextAdjRecast\n    floatPeriodicMaxCap\n    floatPeriodicMinCap\n    floatRoundMethod\n    floatRoundRateFactor\n    floatSendNotice\n    floatStopRecast\n    funds\n    impoundBalance\n    iNFIndexARMUid\n    investAssetNumber\n    lateChargesDays\n    lateChargesPct\n    lenderAccount\n    lienPosition\n    loanAccount\n    loanChargesAccruedInterest\n    loanChargesPrincipal\n    maturityDate\n    nextDueDate\n    noteRate\n    noteType\n    occupancyStatus\n    originalBalance\n    originationDate\n    paidOffDate\n    paidToDate\n    payment\n    paymentImpound\n    paymentReserve\n    poffAcrruedInterest\n    poffAcurredLateCharges\n    poffFromBorrower\n    poffFromEscrow\n    poffFromSuspense\n    poffPaidLateCharges\n    poffPrepayPenalty\n    poffPrincipalBalance\n    poffTotal\n    poffUnpaidCharges\n    poffUnpaidInterest\n    poffUnpaidLateCharges\n    prevAccount\n    principalBalance\n    principalWaived\n    propertyAPN\n    propertyCity\n    propertyState\n    propertyStreet\n    propertytype\n    propertyZip\n    purpose\n    rateType\n    restrictedFunds\n    suspenseBalance\n    section32\n    seniorLoanAmount\n    status\n    statusLender\n    thomasMap\n    unearnedDiscount\n    unpaidCharges\n    unpaidInterest\n    unpaidInterestWaived\n    unpaidLateCharges\n    unpaidLateChargesWaived\n }\n}',
                     'variables': {}}},

                {'name': 'loan_portfolio_information_wla',
                 'request_body': {
                     "query": "{\n  getLoanPortfolio\n  {\n    loanAccount\n    lenderAccount\n    purchaseDate\n    prevServiceAccount\n    origLender\n    investorAssetNumber\n    name\n    city\n    state\n    maturityDate\n    primaryPurpose\n    originalBalance\n    currentBalance\n    daysLate\n    nextDueDate\n    noteRate\n    investorRate\n    totalPayment\n    loanStatus\n    boardingDate\n    closedDate\n    closedReason\n    defaultInterestActiveStatus\n    defaultInterestRate\n    defaultInterestActiveDate\n    lenderOwnerPct\n    brokerName\n    vendor\n    restrictedFunds\n    reserveBalance\n    property{\ncity\nstate\nstreet\nzipCode\n    }\n  }\n}",
                     "variables": {}}},

                {'name': 'payoff_demand_status',
                 'request_body': {
                     'query': '{\n  getPayOffDemandStatus(wasPaid:true dateFrom:\"01-01-2021\") {\n    account\n    borrowerName\n    borrowerAddress\n    borrowerCity\n    borrowerState\n    borrowerZip\n    complianceDate\n    upb\n    interestRate\n    paidToDate\n    nextDueDate\n    maturityDate\n    paidOffDate\n    closedDate\n    propertyState\n    propertyCity\n    propertyZip\n    loanStatus\n    datePayoffDemandQuoteIssued\n    wasPaid\n    forwardToLender\n  }\n}',
                     'variables': {}}},
                     
                {'name': 'getLienReport',
                 'request_body': {
                     'query': '{\n  getLienReport{\n    lenderAccount\n    loanAccount\n    street\n    city\n    state\n    zipCode\n    date\n  }\n}',
                     'variables': {}}},

                {'name': 'getInvoiceList',
                 'request_body': {
                     'query': '{\n    getInvoiceList{\n        numInvoice\n        fullName\n        account\n        isFrozen\n        department\n        amount\n        date\n        dateDue\n        lastDateSent\n    }\n}',
                     'variables': {}}},

                {'name': 'getArmReport',
                 'request_body': {
                     'query': '{\n  getArmReport\n  {\n    account\n    adjForPayment\n    appCreationDate\n    ceiling\n    dayisMonth\n    daysinYear\n    floatFirstRateMinCap\n    floatFreqPymtChange\n    floatPeriodicMaxCap\n    floor\n    indexARMString\n    loanStatus\n    lookBackDays\n    margin\n    newInterestRate\n    newTotalPayment\n    noticeType\n    originationDate\n    propertyState\n    propertyType\n    rateType\n    roundFactor\n    roundMethod\n  }\n}',
                     'variables': {}}},
                
                {'name': 'getACHStatus',
                    'request_body': {
                        'query': '{\ngetACHStatus\n{\nloanAccount\nachStatus\nborrowerName\nnextDebitDate\ncustomPayment\npaymentAmount\n}\n}\n',
                        'variables': {}}},

                {'name': 'getForeclosure',
                    'request_body': {
                        'query': '{\n  getForeclosure{\n    account\n    property\n    followUpDate\n    foreclosureProccess\n    referedForeclosure\n    dateClosed\n    reasonClosed\n    referenceNo\n    company\n    phone\n    overwriteTempo\n    fcOnHold    \n    referredToFC\n    referredToAtty\n    attyReceived\n    noSent\n    noExpires\n    lastPmtReceived\n    pullTitleActual\n    pullTitleProjected\n    complaintFiledActual\n    complaintFiledProjected\n    servedComplaintActual\n    servedComplaintProjected\n    judgmentFiledActual\n    judgmentFiledProjected\n    judgmentGrantedActual\n    judgmentGrantedProjected\n    saleDateActual\n    saleDateProjected\n    biddindInstructionsRequest\n    biddindInstructionsSent\n    saleResults\n    saleAmount\n  }\n}',
                        'variables': {}}},

                {'name': 'getLenderStatement',
                        'request_body': {
                            'query': '{\n    getLenderStatement{\nlenderAccount\nlenderName\nportfolioBalance\nportfolioYield\ndate\ndescription\n    }\n}\n',
                            'variables': {}}},
                    
                {'name': 'getPaymentListToLender',
                            'request_body': {
                                'query': '{\n  getPaymentListToLender{\n   checkDate\n   checkNo\n   checkMemo\n   account\n   lenderAccount\n   paymentType\n   paymentDue\n   checkAmount\n   toServiceFee\n   toInterest\n   toPrincipal\n   toLateCharge\n   toChargesPrincipal\n   toChargesInterest\n   toPrepay\n   toOtherTaxable\n   toOtherTaxFree\n   toOtherPayments\n   toTrust\n   defaultInterest\n   noteInterest\n  }\n}\n',
                                'variables': {}}},

                {'name': 'getBorrowerPayment',
                        'request_body': {
                            'query': '{\n  getBorrowerPayment{\n    account\n    investAssetNumber\n    dateReceived\n    dateDue\n    dayVariance\n    reference\n    isACH\n    paymentType\n    reserveRestricted\n    totalAmount\n    toInterest\n    toPrincipal\n    accruedLateCharges\n    lateChargesPaid\n    toReserve\n    toEscrow\n    toPrepay\n    toChargesPrincipal\n    toChargesInterest\n    toBrokerFee\n    toLenderFee\n    toOtherTaxable\n    toOtherTaxFree\n    toOtherPayments\n    toUnpaidInterest\n    notes\n    uid\n  }\n}',
                            'variables': {}}},
                
                {'name': 'getLoanCashFlow',
                    'request_body': {
                        'query': '{\n  getLoanCashFlow{\n    account\n    assetNumber\n    borrowerName\n    status\n    nextDueDate\n    paymentDate\n    monthToDate\n    lastMonth\n    month2\n    month3\n    month4\n  }\n}\n',
                        'variables': {}}},

                {'name': 'getPayString',
                    'request_body': {
                        'query': '{\n  getPayString{\n    account\n    principalBalance\n    currentDQStatus\n    payString\n  }\n}\n',
                        'variables': {}}},
                
                {'name': 'getNotes',
                    'request_body': {
                    'query': '{\n  getNotes(investor:\"all\"){\n    account\n    noteDate\n    fciRep\n    contactNumber\n    subject\n    noteType\n    contactPerson\n    note\n    borrowerFullName\n  }\n}',
                    'variables': {}}},
                 ]

    loan_list = []
    invoice_list = []
    request_session = requests.Session()
    request_session.mount('https://', HTTPAdapter(max_retries=3))
    
    for r in api_list:
        url = 'https://fapi.myfci.com/graphql'

        if r['name'] in ['getPayString','getLoanCashFlow','getBorrowerPayment','getPaymentListToLender','getLenderStatement','getForeclosure','getACHStatus','loan_portfolio_information_wla', 'payoff_demand_status','getLienReport','getInvoiceList',
                         'getArmReport','getNotes']:
            headers = header2
            url = 'https://fapi.myfci.com/graphql'

        response = request_session.post(url, headers=headers, json=r['request_body'], timeout=120)
        if response.status_code != 200:
            # Handle non-200 responses
            print(f"Request failed with status code {response.status_code} and api failed is {r['name']}")
            print("Response Text:", response.text)
        output = json.loads(response.text)

        if r['name'] == 'loan_portfolio_information_wla':

            data = output['data']['getLoanPortfolio']
            for d in data:
           
                loan_list.append(d['loanAccount'])
        if r['name'] == 'getInvoiceList':

            data = output.get('data').get('getInvoiceList')
            for d in data:
                invoice_list.append(d['numInvoice'])
        file_to_s3(r, output)

    out = {'data': {'getLoanDetails': []}}

    count = 0
    all_tasks = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        for item_index in range(len(loan_list)):
          
            all_tasks.append(
                executor.submit(
                    get_data,
                    loan_list[item_index]
                )
            )

            count = count + 1
        #print("after submittion")
        temp_res = {}
        for future in as_completed(all_tasks):
            #print(future.result())
            loanid, data = future.result()
            if data is None:
                continue
            out['data'][list(out['data'].keys())[0]].append(data['data'][list(out['data'].keys())[0]])
        #print("after as_completed")

    print(count)
    print("number of loans:", len(loan_list))
    print("loan_list", loan_list)
    print("invoice_list", invoice_list)

    file_to_s3({'name': 'loan_details'}, out)
    data_dict = {
    'data': [loan_list, invoice_list] }

    bucket = storage_client.bucket(Dest_bucket)
    try:
        file_name = 'fci/daily_extracts/output/output.json'
        blob = bucket.blob(file_name)
        blob.upload_from_string(json.dumps(data_dict),content_type='application/json')
    except Exception as e:
        print(e)

    return {
        'data': [loan_list, invoice_list],
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
