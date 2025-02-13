import os
env = os.environ.get('stage', 'dev')
sender_email = {"name":"noreply@toorakcapital.com","email":"noreply@toorakcapital.com"}

if env == 'prod':
    
    pst_comparator_recipients = [{"email":'ragarwal@toorakcapital.com',"name":"akshith Agarwal"},
                             {"email":'rmandalapu@toorakcapital.com',"name":"Ram Charan"},
                             {"email":'sgoldman@toorakcapital.com',"name":"Scott Goldman"},
                             {"email":'cmacintosh@toorakcapital.com',"name":"Charles Macintosh"}]
    
    pst_comparator_cc = [{"email":'jkuppuswamy@toorakcapital.com',"name":"Jaya Kuppuswamy"},
                    {"email":'toorakdatateam@triconinfotech.com',"name":"Data Team"}]
    
else:
    
    pst_comparator_recipients = [{"email":'vijaylaxmi@triconinfotech.com',"name":"laxmi"},
                            {"email": "mpinnamaneni@toorakcapital.com", "name": "Mounika Pinnamaneni"}]
    
    pst_comparator_cc = [{"email":'toorakdatateam@triconinfotech.com',"name":"Data Team"}]
    
