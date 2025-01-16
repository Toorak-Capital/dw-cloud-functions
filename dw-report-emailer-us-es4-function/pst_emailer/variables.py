import os
env = os.environ.get('stage', 'dev')
sender_email = {"name":"noreply@toorakcapital.com","email":"noreply@toorakcapital.com"}

if env == 'prod':
    
    pst_report_recipients = [{"email":'pst@toorakcapital.com',"name":"PST Team"}]
    
    pst_report_cc = [{"email":'jkuppuswamy@toorakcapital.com',"name":"Jaya Kuppuswamy"},
                    {"email":'toorakdatateam@triconinfotech.com',"name":"Data Team"}]
    
else:
    
    pst_report_recipients = [{"email":'vijaylaxmi@triconinfotech.com',"name":"laxmi"},
                            {"email": "mpinnamaneni@toorakcapital.com", "name": "Mounika Pinnamaneni"}]
    
    pst_report_cc = [{"email":'toorakdatateam@triconinfotech.com',"name":"Data Team"}]
    
