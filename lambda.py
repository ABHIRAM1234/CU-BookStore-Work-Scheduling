import boto3
import os
from datetime import datetime, timezone

# Initialize the S3 client
s3_client = boto3.client('s3')

# S3 bucket and folder configuration
BUCKET_NAME = 'bookstore-scheduling-bucket'
S3_FOLDER = 'Input/'
EMPLOYEE_FOLDER = 'Daily Employee Availability/'
SHIFT_FOLDER = 'Daily Shift Requirements/'
OUTPUT_FOLDER = 'Processed/'

EMPLOYEE_OUTPUT_FOLDER = f"{OUTPUT_FOLDER}Daily Employee Availability/"
SHIFT_OUTPUT_FOLDER = f"{OUTPUT_FOLDER}Daily Shift Requirements/"

def lambda_handler(event, context):
    try:
        # Get today's date in YYYY-MM-DD format
        today_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        
        # Generate prefixes for today's files in both folders
        employee_prefix = f"{S3_FOLDER}{EMPLOYEE_FOLDER}{today_date}_"
        shift_prefix = f"{S3_FOLDER}{SHIFT_FOLDER}{today_date}_"
        
        # Retrieve files for the given prefixes
        employee_files = retrieve_files_with_prefix(employee_prefix)
        shift_files = retrieve_files_with_prefix(shift_prefix)
        
        if not employee_files and not shift_files:
            return {
                "statusCode": 404,
                "body": f"No files found for date: {today_date} in folders."
            }
        
        reuploaded_files = []
        
        for file_key in employee_files:
            local_file_path = f"/tmp/{file_key.split('/')[-1]}"
            s3_client.download_file(BUCKET_NAME, file_key, local_file_path)
            
            # Re-upload to Employee Availability folder
            new_s3_key = f"{EMPLOYEE_OUTPUT_FOLDER}{file_key.split('/')[-1]}"
            s3_client.upload_file(local_file_path, BUCKET_NAME, new_s3_key)
            reuploaded_files.append(f"s3://{BUCKET_NAME}/{new_s3_key}")
            print(f"Uploaded file: {local_file_path} to S3 at {new_s3_key}")
            os.remove(local_file_path)  # Clean up

        for file_key in shift_files:
            local_file_path = f"/tmp/{file_key.split('/')[-1]}"
            s3_client.download_file(BUCKET_NAME, file_key, local_file_path)
            
            # Re-upload to Shift Requirements folder
            new_s3_key = f"{SHIFT_OUTPUT_FOLDER}{file_key.split('/')[-1]}"
            s3_client.upload_file(local_file_path, BUCKET_NAME, new_s3_key)
            reuploaded_files.append(f"s3://{BUCKET_NAME}/{new_s3_key}")
            print(f"Uploaded file: {local_file_path} to S3 at {new_s3_key}")
            os.remove(local_file_path)  # Clean up
        
        return {
            "statusCode": 200,
            "body": f"Reuploaded files are available at: {reuploaded_files}"
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            "statusCode": 500,
            "body": f"An error occurred: {str(e)}"
        }

def retrieve_files_with_prefix(prefix):
    """
    Helper function to retrieve files with a given prefix from the S3 bucket.
    """
    try:
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
        if 'Contents' in response:
            return [obj['Key'] for obj in response['Contents']]
        else:
            return []
    except Exception as e:
        print(f"Error retrieving files with prefix {prefix}: {e}")
        return []

