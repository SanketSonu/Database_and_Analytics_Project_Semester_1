import io
import pandas as pd
import boto3
import boto.s3.connection
from botocore.exceptions import NoCredentialsError
import csv
import os

from dagster import pipeline, solid


@solid
def upload_to_s3(context, list_of_files):
    bucket='slark'
    context.log.info(f"** Initiating upload of file(s) {list_of_files} to {bucket} S3 bucket in AWS")
    ACCESS_KEY = 'AKIAQGMFMSSI4DZTTKXB'  # Access Key of User
    SECRET_KEY = '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'  # Secret Key of User

    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,  # Calling client with Credentials to get connection.
                          aws_secret_access_key=SECRET_KEY)

    try:
        for file_name in list_of_files:
            s3.upload_file(file_name, bucket, file_name)  # Function to upload a file.
            context.log.info(f"** {file_name} was Uploaded Successfully to S3")
        return True
    except FileNotFoundError:
            print("The file was not found")
            return False
    except NoCredentialsError:
            print("Credentials not available")
            return False

@solid
def get_file_List(context):
   return ['Covid_dataset1.csv','Covid_dataset2.csv','Covid_dataset3.csv','world_countries.json']
   # return ['DAP_sam1_up.csv','DAP_sam2_up.csv','DAP_sam3_up.csv']


@pipeline
def upload_to_s3_pipeline():
    #solid to upload dataset to AWS S3
    upload_to_s3(get_file_List())

