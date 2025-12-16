import boto3
import pandas as pd
from io import BytesIO

def extract_from_minio():
    s3 = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minio',
        aws_secret_access_key='minio123'
    )
    obj = s3.get_object(Bucket='datasets', Key='Reviews.csv')
    df = pd.read_csv(BytesIO(obj['Body'].read()))
    return df
