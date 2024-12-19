import os
env = os.environ.get('stage', 'dev')
sender_email = {"name":"noreply@toorakcapital.com","email":"noreply@toorakcapital.com"}


table_funding_look_ids = {'loan_info': 243,
            'summary': 244,
            }

if env == 'prod':
    
    table_funding_recipients = [{"email":'Closing@toorakcapital.com',"name":"Closing"},
                        {"email":'tfloanscenarios@toorakcapital.com',"name":"Table_Funding_Loan_Scenarios"},
                        {"email":'LAT@toorakcapital.com',"name":"Loan_Acq_Team"}]
    
    table_funding_cc = [{"email":'jkuppuswamy@toorakcapital.com',"name":"jkuppuswamy"},
                        {"email":'mpinnamaneni@toorakcapital.com',"name":"mpinnamaneni"},
                        {"email":'vijaylaxmi@triconinfotech.comm',"name":"vijaylaxmi"},
                        {"email":'toorakdatateam@triconinfotech.com',"name":"toorakdatateam"}]
    
else:
    
    table_funding_recipients = [{"email":'mpinnamaneni@toorakcapital.com',"name":"mpinnamaneni"},
                        {"email":'vijaylaxmi@triconinfotech.comm',"name":"vijaylaxmi"}]

    table_funding_cc = [{"email":'toorakdatateam@triconinfotech.com',"name":"toorakdatateam"}]


