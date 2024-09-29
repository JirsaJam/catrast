import boto3
import os

def list_s3_files_without_extension(bucket_name, prefix, profile_name):
    session = boto3.Session(profile_name=profile_name)
    
    s3 = session.client('s3')
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    
    if 'Contents' in response:
        file_names = []
        for obj in response['Contents']:
            key = obj['Key']
            
            filename = os.path.basename(key)
            
            if filename:  # Ensure it's a file, not just a folder
                file_name_without_extension = os.path.splitext(filename)[0]
                file_names.append(file_name_without_extension)
        
        return file_names
    else:
        print('No files found.')
        return []

# Usage
bucket_name = 'gsr-landfire'
prefix = '2023/lf/categorical /input/'
profile_name = 'se'  # The profile you've configured using AWS SSO

file_names_without_extension = list_s3_files_without_extension(bucket_name, prefix, profile_name)
print(file_names_without_extension)

