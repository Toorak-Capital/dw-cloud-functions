import os
env = os.environ.get('stage', 'dev')
sender_email = {"name":"noreply@toorakcapital.com","email":"noreply@toorakcapital.com"}

# twtr_look_ids = {'purchases_1': 181,
#             'purchases_2': 175,
#             'wac' : 174,
#             'rolling_4_1' : 176,
#             'rolling_4_2' : 177,
#             'rolling_4_3' : 178,
#             'rolling_8_1' : 179,
#             'rolling_8_2' : 180,
#             'graph_1' : 173,
#             'graph_2' : 171,
#             'graph_3' : 172}



if env == 'prod':
    
    twtr_email_recipients = [{"email":'edyner@toorakcapital.com',"name":"Eric"},
                             {"email":'svenugopal@toorakcapital.com',"name":"Sachin"},
                             {"email":'jkuppuswamy@toorakcapital.com',"name":"Jaya"},
                             {"email":'kgood@toorakcapital.com',"name":"Kyle"},
                             {"email":'mpinnamaneni@toorakcapital.com',"name":"Mounika"},
                             {"email":'vijaylaxmi@triconinfotech.com',"name":"Laxmi"}]
    
    twtr_email_cc = [{"email":'hsharma@toorakcapital.com',"name":"Data Team"}]
    
else:
    
    twtr_email_recipients = [{"email":'hsharma@toorakcapital.com',"name":"Eric"}]
    
    twtr_email_cc = [{"email":'mpinnamaneni@toorakcapital.com',"name":"Mounika"},
                    {"email":'vijaylaxmi@triconinfotech.com',"name":"Laxmi"}]
    
