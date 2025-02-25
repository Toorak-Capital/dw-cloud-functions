import os
import pysftp
from google.cloud import storage
from variables import *
import ast
from google.cloud import secretmanager_v1
import json
import base64
import hashlib

def upload_to_gcs(bucket_name, file_name, local_path):
    """Uploads a file to GCS bucket only if it's new or updated."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(f"Fay/{file_name}")

    if blob.exists(client):
        existing_md5 = base64.b64decode(blob.md5_hash).hex()  # Convert base64 MD5 to hex
        local_md5 = calculate_md5(local_path)

        if existing_md5 == local_md5:
            print(f"Skipping {file_name}, no changes detected.")
            return
    
    blob.upload_from_filename(local_path)
    print(f"Uploaded {file_name} to {bucket_name}")

def calculate_md5(file_path):
    """Calculates MD5 checksum of a file."""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def get_secret(secret_id):
    """Fetches secrets from Google Secret Manager."""
    client = secretmanager_v1.SecretManagerServiceClient()
    name = f"projects/{secret_project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(name=name)
    secret_data = response.payload.data.decode("UTF-8")

    try:
        return json.loads(secret_data)  # Return JSON object if possible
    except json.JSONDecodeError:
        return secret_data  # Otherwise, return as plain text

def fetch_sftp_files(event):
    """Fetches files from SFTP and uploads them to GCS if new or updated."""
    remote_dir = "/fayservicing/Toorak/"
    local_dir = "/tmp"
    
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None

    fay_sftp_credentials = get_secret(fay_sftp_credentials_key)
    fay_sftp_credentials = ast.literal_eval(fay_sftp_credentials)

    try:
        with pysftp.Connection(fay_sftp_credentials['host'], username=fay_sftp_credentials['username'], password=fay_sftp_credentials['password'], cnopts=cnopts) as sftp:
            with sftp.cd(remote_dir):
                files = [file for file in sftp.listdir_attr() if not file.longname.startswith("d") and file.filename.startswith("Fay_")]
                
                for file in files:
                    remote_file_path = f"{remote_dir}/{file.filename}"
                    local_file_path = os.path.join(local_dir, file.filename)

                    sftp.get(remote_file_path, local_file_path)  # Download file
                    print(f"Downloaded to Local: {file.filename}")

                    upload_to_gcs(gcs_bucket, file.filename, local_file_path)  # Upload to GCS if new or updated

                    os.remove(local_file_path)  # Cleanup local file
    except Exception as e:
        print(f"Encountered an error processing file: {file.filename if file in locals() else 'Unknown'}. Error Details: {e}")

    return {
        'statusCode': 200,
        'body': json.dumps('Completed copying all the files.')
    }
