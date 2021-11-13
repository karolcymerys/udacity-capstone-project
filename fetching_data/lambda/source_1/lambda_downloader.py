import json
import logging
import os
from io import BytesIO

import boto3
import requests

logger = logging.getLogger()
logger.setLevel(os.getenv('LOGGING_INFO', 'INFO'))

S3_BUCKET_NAME = None
s3_client = None
lambda_initialized: bool = False


def init():
    global S3_BUCKET_NAME, s3_client, lambda_initialized
    if not lambda_initialized:
        S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
        s3_client = boto3.client('s3')
        lambda_initialized = True


def handler(event, context):
    init()
    message = json.loads(event['Records'][0]['body'])
    source = message['source']
    destination = message['destination']

    logger.info(f'Fetching data to: {source}')
    response = requests.get(source)
    logger.info(f'Saving data to S3: {destination}')
    s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=destination, Body=BytesIO(response.content))
    logger.info(f'Data saved in S3: {destination}')
