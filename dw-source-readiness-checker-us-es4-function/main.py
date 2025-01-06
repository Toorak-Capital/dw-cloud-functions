from google.cloud import storage
from prettytable import PrettyTable
import pandas as pd
import urllib
import os
from datetime import *
import json
import requests
import urllib
from variables import *
from google.cloud import storage
import google.auth.transport.requests
import google.oauth2.id_token

def get_latest_file_in_folder(bucket_name, folder_prefix):
    # Initialize a GCS client
    client = storage.Client()
    
    # Get the bucket
    bucket = client.get_bucket(bucket_name)
    
    # List blobs in the specified folder
    blobs = bucket.list_blobs(prefix=folder_prefix)
    
    # Find the latest blob
    latest_blob = None
    latest_time = None
    
    for blob in blobs:
        if latest_time is None or blob.updated > latest_time:
            latest_blob = blob
            latest_time = blob.updated
    
    return latest_blob, latest_time

def check_latest_file_date(bucket_folder_pairs_source):
    # Create a PrettyTable object for nice output formatting
    table = PrettyTable(['Servicers', 'Buffer Days', 'Latest File', 'Last Modified Time'])
    
    # List to store the results for DataFrame creation
    result_list = []
    
    # Track whether all folders have files within the buffer days
    all_files_up_to_date = True
    
    # Loop through the folder pairs and check the latest file's date
    for folder_alias_dict in bucket_folder_pairs_source:
        for folder_alias, (bucket_name, folder_prefix, buffer_days) in folder_alias_dict.items():
            # Calculate the target date based on buffer days
            target_date = (datetime.now(timezone.utc) - timedelta(days=buffer_days)).date()
            
            latest_blob, latest_time = get_latest_file_in_folder(bucket_name, folder_prefix)
            
            if latest_blob:
                latest_file_date = latest_time.date()
                
                if latest_file_date < target_date:
                    # Add to the table if the latest file is not within buffer days
                    table.add_row([folder_alias, buffer_days, latest_blob.name.split('/')[-1], latest_file_date])
                    result_list.append({
                        'Servicers': folder_alias,
                        'Buffer Days': buffer_days,
                        'Latest File': latest_blob.name.split('/')[-1],
                        'Last Modified Time': latest_file_date
                    })
                    all_files_up_to_date = False
            else:
                # No files found in the folder
                table.add_row([folder_alias, buffer_days, 'No files found', 'N/A'])
                result_list.append({
                    'Servicers': folder_alias,
                    'Buffer Days': buffer_days,
                    'Latest File': 'No files found',
                    'Last Modified Time': 'N/A'
                })
                all_files_up_to_date = False
    
    # Print the table
    print("Folders with files not within buffer days:")
    print(table)
    
    # Convert the result list to a DataFrame
    df = pd.DataFrame(result_list)
    
    return df, table

def create_and_upload_log_file(bucket_name, file_name, log_content):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_string(log_content)
    print(f"Log file '{file_name}' uploaded to bucket '{bucket_name}' successfully.")


def lambda_handler(event):
    current_date = datetime.now().strftime("%Y-%m-%d")
    file_name = f"{current_date}-file-checker.log"
    event_json = event.get_json(silent=True)
    if event_json['files'] == 'rsd_files':
        mismatches, pretty_table = check_latest_file_date(rsd_bucket_folder_pairs_dest)
    else:
        mismatches, pretty_table = check_latest_file_date(bucket_folder_pairs_dest)
    if not mismatches.empty:
        print(mismatches)
        create_and_upload_log_file(f'dw-{env}-cron-job-log-file-execution', f'{file_name}','Stop the Pipeline')
        html_table = pretty_table.get_html_string(attributes={"border": "1", "class": "table table-striped"})
        print(html_table)
        email_body = EmailBody.replace('{{html_table}}', html_table)

        params = {
            "subject": Subject,
            "to": Recipients,
            "body": email_body
        }

        req = urllib.request.Request(SendEmailCFUrl)
        auth_req = google.auth.transport.requests.Request()
        auth_token = google.oauth2.id_token.fetch_id_token(auth_req, SendEmailCFUrl)

        try:
            headers = {'Authorization': f'Bearer {auth_token}'}
            response = requests.post(SendEmailCFUrl, json=params, headers=headers)
            if response.status_code == 200:
                print("Cloud Function invoked successfully.")
                print("Response:", response.text)
                return {
                    'statusCode': 200,
                    'body': json.dumps('Mismatches found.')
                }
            else:
                print("Failed to invoke Cloud Function. Status code:", response.status_code)
                return {
                    'statusCode': 200,
                    'body': json.dumps('Mismatches found.')
    }
        except Exception as e:
            print("An error occurred:", e)

    else:
        print("No mismatches found.")
        return {
            'statusCode': 200,
            'body': json.dumps('No mismatches found.')
    }