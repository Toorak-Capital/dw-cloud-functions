import os
env = os.environ.get('stage', 'dev')
sender_email = {"name":"noreply@toorakcapital.com","email":"noreply@toorakcapital.com"}

risk_score_look_ids = {'monthly_report': 233,
            'weekly_report': 232}



if env == 'prod':
    
    risk_score_recipients = [{"email":'lberger@toorakcapital.com',"name":"Lincoln Berger"},
                                    {"email":'edyner@toorakcapital.com',"name":"Eric Dyner"},
                                    {"email":'jward@toorakcapital.com',"name":"Jason Ward"},
                                    {"email":'mbergamaschi@toorakcapital.com',"name":"Mauricio Bergamaschi"}]
    
    risk_score_cc = [{"email":'jkuppuswamy@toorakcapital.com',"name":"Jaya Kuppuswamy"},
                    {"email":'toorakdatateam@triconinfotech.com',"name":"Data Team"}]
    
else:
    
    risk_score_recipients = [{"email":'vijaylaxmi@triconinfotech.com',"name":"laxmi"},
                            {"email": "mpinnamaneni@toorakcapital.com", "name": "Mounika Pinnamaneni"}]
    
    risk_score_cc = [{"email":'toorakdatateam@triconinfotech.com',"name":"Data Team"}]
    
