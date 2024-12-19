import os
env = os.environ.get('stage', 'dev')
sender_email = {"name":"noreply@toorakcapital.com","email":"noreply@toorakcapital.com"}


uk_look_ids = {'table_1': 158,
            'table_2': 159,
            'table_3' : 160,
            'table_4' : 162,
            'table_5' : 164,
            'table_6' : 165,
            'graph_1' : 166,
            'graph_2' : 167,
            'graph_3' : 168,
            'graph_4' : 169,
            'graph_5' : 170,
            }

if env == 'prod':
    
    uk_email_recipients = [{"email":'asimanovsky@toorakcapital.com',"name":"Aleksandra"},
                        {"email":'emacan@toorakcapital.com',"name":"Edward"},
                        {"email":'jkuppuswamy@toorakcapital.com',"name":"Jaya"}]
    
    uk_email_cc = [{"email":'toorakdatateam@triconinfotech.com',"name":"Data Team"}]
    
else:
    
    uk_email_recipients = [{"email":'vijaylaxmi@triconinfotech.com',"name":"laxmi"},
                            {"email": "mpinnamaneni@toorakcapital.com", "name": "Mounika Pinnamaneni"}]
    
    uk_email_cc = [{"email":'toorakdatateam@triconinfotech.com',"name":"Data Team"}]

