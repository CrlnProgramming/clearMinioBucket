import boto3
from botocore.client import Config
from datetime import datetime, timedelta, timezone
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

minio_endpoint = ''
access_key = ''
secret_key = ''
bucket_name = ''

time_threshold = datetime.now(timezone.utc) - timedelta(days=7)

s3_client = boto3.client(
    's3',
    endpoint_url=minio_endpoint,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

def clear_bucket_by_time(bucket_name, time_threshold):
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket_name)

        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['LastModified'] < time_threshold:
                        s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
                        logger.info(f"Deleted {obj['Key']} (LastModified: {obj['LastModified']})")
            else:
                logger.info("Bucket is already empty")
                break
    except Exception as e:
        logger.error(f"An error occurred: {e}")

clear_bucket_by_time(bucket_name, time_threshold)
