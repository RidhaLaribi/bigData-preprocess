import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import boto3
from io import BytesIO
from sqlalchemy import create_engine

# ======================
# Task functions
# ======================

def clean_tmp_files():
    # remove old temp files
    for f in ['/tmp/clean.parquet', '/tmp/trans.parquet', '/tmp/transformed.parquet', '/tmp/final.parquet']:
        if os.path.exists(f):
            os.remove(f)

def extract(ti, **kwargs):
    # get file from MinIO
    s3 = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minio',
        aws_secret_access_key='minio123'
    )
    obj = s3.get_object(Bucket='datasets', Key='datacv19.csv')
    df = pd.read_csv(BytesIO(obj['Body'].read()))
    df.to_parquet("/tmp/clean.parquet")
    ti.xcom_push(key='message', value='extracted')

def clean(ti, **kwargs):
    df = pd.read_parquet("/tmp/clean.parquet")
    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)
    # remove outliers
    for col in df.select_dtypes(include='number'):
        q1, q3 = df[col].quantile([0.25, 0.75])
        iqr = q3 - q1
        df = df[(df[col] >= q1 - 1.5*iqr) & (df[col] <= q3 + 1.5*iqr)]
    df.to_parquet("/tmp/trans.parquet")


def transform_data(ti,**kwargs):
    df = pd.read_parquet("/tmp/trans.parquet")
    # ti.xcom_push(key='col', value=df.select_dtypes(include='number').columns)

    for col in df.select_dtypes(include='number').columns:
        mean, std = df[col].mean(), df[col].std()
        df[col] = (df[col]-mean)/std if std != 0 else 0.0
    df = pd.get_dummies(df)
    df.to_parquet("/tmp/transformed.parquet")
    # ti.xcom_push(key='transform_status', value='done')

def aggregate_data(ti,**kwargs):
    import os
    import pandas as pd

    file_path = "/tmp/transformed.parquet"
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"{file_path} not found")

    df = pd.read_parquet(file_path)
    if df.shape[1] == 0:
        raise ValueError("Empty dataframe")

    key_col = df.columns[0]
    agg = df.groupby(key_col, as_index=False).mean(numeric_only=True)

    agg.to_parquet("/tmp/final.parquet")
    ti.xcom_push(key='aggregate_status', value='done')

def load_to_postgres(**kwargs):
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/analytics')
    df = pd.read_parquet('/tmp/final.parquet')
    df.to_sql('final_table', engine, if_exists='replace', index=False)


with DAG(
    'final_pipeline',
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
