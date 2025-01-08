import os
env = os.environ.get('stage', 'dev')
sender_email = {"name":"noreply@toorakcapital.com","email":"noreply@toorakcapital.com"}

merchant_look_ids = {'purchases_1': 181,
            'purchases_2': 175,
            'wac' : 174,
            'rolling_4_1' : 176,
            'rolling_4_2' : 177,
            'rolling_4_3' : 178,
            'rolling_8_1' : 179,
            'rolling_8_2' : 180,
            'graph_1' : 173,
            'graph_2' : 171,
            'graph_3' : 172}



if env == 'prod':
    
    merchant_email_recipients = [{"email":'asimanovsky@toorakcapital.com',"name":"Aleksandra"},
                        {"email":'dyner@toorakcapital.com',"name":"Eric"},
                        {"email":'mbergamaschi@toorakcapital.com',"name":"Mauricio"},
                        {"email":'jkuppuswamy@toorakcapital.com',"name":"Jaya"},
                        {"email":"kchen@toorakcapital.com", "name":"kchen"}]
    
    merchant_email_cc = [{"email":'toorakdatateam@triconinfotech.com',"name":"Data Team"}]
    
else:
    
    merchant_email_recipients = [{"email":'vijaylaxmi@triconinfotech.com',"name":"laxmi"},
                            {"email": "mpinnamaneni@toorakcapital.com", "name": "Mounika Pinnamaneni"}]
    
    merchant_email_cc = [{"email":'toorakdatateam@triconinfotech.com',"name":"Data Team"}]
    
