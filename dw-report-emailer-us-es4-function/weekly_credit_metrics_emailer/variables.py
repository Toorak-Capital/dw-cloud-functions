import os
env = os.environ.get('stage', 'dev')
sender_email = {"name":"noreply@toorakcapital.com","email":"noreply@toorakcapital.com"}



if env == 'prod':

    cm_email_recipients = [{"email":"jward@toorakcapital.com","name":"lberger"},
                           {"email":"lberger@toorakcapital.com","name":"Json"},
                            {"email":'jkuppuswamy@toorakcapital.com',"name":"Jaya"},
                             {"email":'kgood@toorakcapital.com',"name":"Kyle"},
                             {"email":'mpinnamaneni@toorakcapital.com',"name":"Mounika"},
                             {"email":'vijaylaxmi@triconinfotech.com',"name":"Laxmi"}]

    cm_email_cc = [{"email":'ToorakDataTeam@triconinfotech.com',"name":"Data Team"}]

else:

    cm_email_recipients = [{"email":'mpinnamaneni@toorakcapital.com',"name":"Mounika"},
                    {"email":'vijaylaxmi@triconinfotech.com',"name":"Laxmi"}]

    cm_email_cc = [{"email":'ToorakDataTeam@triconinfotech.com',"name":"Data Team"}]