import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import boto3
from io import BytesIO

def extract(ti, **kwargs):
    # مجلد الحفظ
    local_dir = '/opt/airflow/data'
    os.makedirs(local_dir, exist_ok=True)  # ← هنا يخلق المجلد إذا لم يكن موجود

    local_file_path = os.path.join(local_dir, 'Reviews.csv')

    s3 = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minio',
        aws_secret_access_key='minio123'
    )

    bucket_name = 'datasets'
    file_key = 'Reviews.csv'

    # s3.download_file(Bucket=bucket_name, Key=file_key, Filename=local_file_path)
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    df = pd.read_csv(BytesIO(obj['Body'].read()))

    print(f"File downloaded to {local_file_path}")
    df.to_parquet("/tmp/clean.parquet")
    ti.xcom_push(key='message', value='exted')

def clean(ti, **kwargs):
    df = pd.read_parquet("/tmp/clean.parquet")
    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)

    for col in df.select_dtypes(include='number'):
        q1 = df[col].quantile(0.25)
        q3 = df[col].quantile(0.75)
        iqr = q3 - q1
        df = df[(df[col] >= q1 - 1.5 * iqr) & (df[col] <= q3 + 1.5 * iqr)]

    df.to_parquet("/tmp/trans.parquet")
    ti.xcom_push(key='message', value='Cleaned')


from sklearn.preprocessing import StandardScaler
import pandas as pd

def transform_data(ti, **kwargs):
    df = pd.read_csv('/tmp/trans.parquet')

    num_cols = df.select_dtypes(include='number')
    # scaler = StandardScaler()
    # try :
    #     df[num_cols.columns] = scaler.fit_transform(num_cols)
    #
    #     df = pd.get_dummies(df)
    # except : pass
    df.to_parquet("/tmp/transformed.parquet")
    ti.xcom_push(key='message', value='exted')

def aggregate_data():
    df = pd.read_csv('/tmp/transformed.parquet')

    agg = df.groupby(df.columns[0]).mean()

    df.to_parquet("/tmp/transformed.parquet")

import pandas as pd
from sqlalchemy import create_engine

def load_to_postgres():
    engine = create_engine(
        'postgresql://airflow:airflow@postgres:5432/analytics'
    )

    df = pd.read_csv('/tmp/final.csv')
    df.to_sql('final_table', engine, if_exists='replace', index=False)



with DAG(
    'test_pipeline',
    start_date=datetime(2025, 12, 16),
    schedule_interval=None,
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id='extracting_data',
        python_callable=extract
    )

    t2 = PythonOperator(
        task_id='cleanning_data',
        python_callable=clean
    )
    t3 = PythonOperator(
        task_id='transforming_data',
        python_callable=transform_data
    )
    t4 = PythonOperator(
        task_id='agregation',
        python_callable=aggregate_data
    )

    t5 = PythonOperator(
        task_id='loading',
        python_callable=load_to_postgres
    )

    t1 >> t2 >> t3 >> t4 >> t5
