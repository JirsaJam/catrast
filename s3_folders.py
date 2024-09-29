import boto3
import os

def list_s3_files_without_extension(bucket_name, prefix):
    s3 = boto3.client('s3')
    
    # List objects in the specified bucket and prefix
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    
    # Check if the response contains the 'Contents' key, meaning there are files
    if 'Contents' in response:
        # Iterate over each file
        file_names = []
        for obj in response['Contents']:
            # Get the full file key (path + filename)
            key = obj['Key']
            
            # Extract the filename from the key (ignore folders)
            filename = os.path.basename(key)
            
            if filename:  # Ensure it's a file, not just a folder
                # Split the filename and extension, then keep the name
                file_name_without_extension = os.path.splitext(filename)[0]
                file_names.append(file_name_without_extension)
        
        return file_names
    else:
        print('No files found.')
        return []

# Usage
bucket_name = 'gsr-landfire'
prefix = '2023/lf/categorical /input/'

file_names_without_extension = list_s3_files_without_extension(bucket_name, prefix)
print(file_names_without_extension)
