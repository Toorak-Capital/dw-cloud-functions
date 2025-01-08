import os
env = os.environ.get('stage', 'dev')

diligence_submission_id = 197

sender_email = {"name":"noreply@toorakcapital.com","email":"noreply@toorakcapital.com"}

if env == 'prod':
        
    opus_email_recipients = [{"email":'Daniel.Chang@opuscmc.com',"name":"Daniel"},
                        {"email":'Cheryl.Peregrine@opuscmc.com',"name":"Cheryl"}]
    
    opus_email_cc=[{"email":'tablefunding@toorakcapital.com',"name":"Table Funding Team"},
                   {"email":'LAT@toorakcapital.com',"name":"Loan Acquisitions Team"},
                    {"email":'jkuppuswamy@toorakcapital.com',"name":"Jaya"},
                        {"email":'toorakdatateam@triconinfotech.com',"name":"Data Team"},
                   {"email":'tooraksupport@triconinfotech.com',"name":"support_team"}]
    
    ca_email_recipients = [{"email":'caanalystsreporting@ca-usa.com',"name":"CA Team"},
                        {"email":'toorakdscr@ca-usa.com',"name":"ToorakDSCR"},
                        {"email":'ToorakRTL@ca-usa.com',"name":"ToorakRTL"}]
    
    ca_email_cc = [{"email":'LAT@toorakcapital.com',"name":"Loan Acquisitions Team"},
                    {"email":'jkuppuswamy@toorakcapital.com',"name":"Jaya"},
                    {"email":'toorakdatateam@triconinfotech.com',"name":"Data Team"},
                   {"email":'tooraksupport@triconinfotech.com',"name":"support_team"}]
    
else:
    
    opus_email_recipients = [{"email":'vijaylaxmi@triconinfotech.com',"name":"laxmi"},
                            {"email": "mpinnamaneni@toorakcapital.com", "name": "Mounika Pinnamaneni"}]
    
    opus_email_cc = [{"email":'toorakdatateam@triconinfotech.com',"name":"Data Team"}]
    
    ca_email_recipients = [{"email":'vijaylaxmi@triconinfotech.com',"name":"laxmi"},
                            {"email": "mpinnamaneni@toorakcapital.com", "name": "Mounika Pinnamaneni"}]
    
    ca_email_cc = [{"email":'toorakdatateam@triconinfotech.com',"name":"Data Team"}]
        
