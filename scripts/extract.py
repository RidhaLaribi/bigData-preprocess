import boto3
<<<<<<< HEAD
import pandas as pd
from io import BytesIO
=======
>>>>>>> 8f7e88994e8af728660a438e6f6ced615e4b67c2

def extract_from_minio():
    s3 = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minio',
        aws_secret_access_key='minio123'
    )
<<<<<<< HEAD
    obj = s3.get_object(Bucket='datasets', Key='Reviews.csv')
    df = pd.read_csv(BytesIO(obj['Body'].read()))
    return df
=======

    s3.download_file('datasets', 'data.csv', '/tmp/data.csv')
>>>>>>> 8f7e88994e8af728660a438e6f6ced615e4b67c2
