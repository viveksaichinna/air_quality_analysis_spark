import boto3
import os
import gzip
import shutil
import csv
from botocore import UNSIGNED
from botocore.config import Config

def download_s3_folder(bucket_name, s3_folder, local_dir):
    """Download all files from an S3 folder to a local directory."""
    
    # Use the UNSIGNED config for public bucket access
    s3 = boto3.client(
        's3',
        region_name='us-east-1',  # adjust the region as needed
        config=Config(signature_version=UNSIGNED)
    )

    # Ensure the local directory exists
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)

    try:
        # List all objects in the specified S3 folder
        result = s3.list_objects_v2(Bucket=bucket_name, Prefix=s3_folder)

        # Check if there are any files in the folder
        if 'Contents' not in result:
            print(f"No files found in {s3_folder}.")
            return

        for obj in result['Contents']:
            s3_file_key = obj['Key']
            local_file_path = os.path.join(local_dir, os.path.basename(s3_file_key))
            
            # Download the file from S3 to the local machine
            print(f"Downloading {s3_file_key} to {local_file_path}")
            s3.download_file(bucket_name, s3_file_key, local_file_path)

        print("All files downloaded successfully in", local_dir)

    except Exception as e:
        print(f"Error downloading files from {s3_folder}: {e}")

def unzip_files_in_dir(local_dir):
    """
    Unzip all .csv.gz files in the specified local directory and save them as .csv files.
    After unzipping, delete the .csv.gz files.
    """
    for filename in os.listdir(local_dir):
        if filename.endswith('.csv.gz'):
            gz_file_path = os.path.join(local_dir, filename)
            csv_file_path = os.path.join(local_dir, filename[:-3])  # remove the .gz extension
            
            print(f"Unzipping {gz_file_path} to {csv_file_path}")
            try:
                with gzip.open(gz_file_path, 'rb') as f_in:
                    with open(csv_file_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                
                # After successful unzip, remove the .csv.gz file
                os.remove(gz_file_path)
                print(f"Successfully unzipped and deleted {filename}")

            except Exception as e:
                print(f"Error unzipping {filename}: {e}")

def process_metadata(metadata_file, bucket_name, base_local_dir):
    """
    Process the locations_metadata.csv file which contains location_id, start_year, and end_year fields.
    For each row and each year between start_year and end_year (inclusive), 
    download and then unzip files from the corresponding S3 folder.
    All files will be saved in the base_local_dir.
    """
    with open(metadata_file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            locationid = row['location_id']
            try:
                start_year = int(row['start_year'])
                end_year = int(row['end_year'])
            except ValueError as e:
                print(f"Skipping row with invalid year values: {row} - {e}")
                continue

            for year in range(start_year, end_year + 1):
                # Build the S3 folder path.
                s3_folder = f"records/csv.gz/locationid={locationid}/year={year}/"
                
                print(f"\nProcessing for country {row['country']} location {locationid} for year {year}")
                download_s3_folder(bucket_name, s3_folder, base_local_dir)
                unzip_files_in_dir(base_local_dir)

if __name__ == '__main__':
    # Define the S3 bucket details
    bucket_name = 'openaq-data-archive'
    
    # Path for the metadata CSV file
    metadata_file = 'locations_metadata.csv'
    
    # Base local directory where downloads will be saved (all in one directory)
    base_local_dir = 'data/pending'
    
    # Ensure the base directory exists
    if not os.path.exists(base_local_dir):
        os.makedirs(base_local_dir)
    
    # Process the metadata file and perform downloads and unzipping
    process_metadata(metadata_file, bucket_name, base_local_dir)
