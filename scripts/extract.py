import boto3

def extract_from_minio():
    s3 = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minio',
        aws_secret_access_key='minio123'
    )

    s3.download_file('datasets', 'data.csv', '/tmp/data.csv')
