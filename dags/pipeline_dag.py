
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def test_task(**kwargs):
    value = "Hello XCom"
    kwargs['ti'].xcom_push(key='message', value=value)

with DAG(
    'test_pipeline',
    start_date=datetime(2025, 12, 16),
    schedule_interval=None,
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id='hello_task',
        python_callable=test_task,
        provide_context=True  # مهم للحصول على kwargs
    )












#
#
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime
<<<<<<< HEAD
#
# def hello_task(ti, **kwargs):
#     ti.xcom_push(key='message', value='Hello XCom')
#
# def print_xcom(ti, **kwargs):
#     msg = ti.xcom_pull(key='message', task_ids='hello_task')
#     print("Got from XCom:", msg)
#
# with DAG(
#     'test_pipeline',
#     start_date=datetime(2025, 12, 16),
=======
# from scripts.extract import extract_from_minio
# from scripts.clean import clean_data
# from scripts.transform import transform_data
# from scripts.aggregate import aggregate_data
# from scripts.load import load_to_postgres
#
# with DAG(
#     'bigdata_pipeline',
#     start_date=datetime(2025, 12, 15),
>>>>>>> 8f7e88994e8af728660a438e6f6ced615e4b67c2
#     schedule_interval=None,
#     catchup=False
# ) as dag:
#
<<<<<<< HEAD
#     t1 = PythonOperator(
#         task_id='hello_task',
#         python_callable=hello_task
#     )
#
#     t2 = PythonOperator(
#         task_id='print_xcom',
#         python_callable=print_xcom
#     )
#
#     t1 >> t2


import pandas as pd

def aggregate_data():
    df = pd.read_csv('/tmp/transformed.csv')

    agg = df.groupby(df.columns[0]).mean()

    agg.to_csv('/tmp/final.csv')

import pandas as pd

def clean_data():
    df = pd.read_csv('/tmp/data.csv')

    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)

    for col in df.select_dtypes(include='number'):
        q1 = df[col].quantile(0.25)
        q3 = df[col].quantile(0.75)
        iqr = q3 - q1
        df = df[(df[col] >= q1 - 1.5 * iqr) & (df[col] <= q3 + 1.5 * iqr)]

    df.to_csv('/tmp/clean.csv', index=False)


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


import pandas as pd
from sqlalchemy import create_engine

def load_to_postgres():
    engine = create_engine(
        'postgresql://airflow:airflow@postgres:5432/analytics'
    )

    df = pd.read_csv('/tmp/final.csv')
    df.to_sql('final_table', engine, if_exists='replace', index=False)

from sklearn.preprocessing import StandardScaler
import pandas as pd

def transform_data():
    df = pd.read_csv('/tmp/clean.csv')

    num_cols = df.select_dtypes(include='number')
    scaler = StandardScaler()
    df[num_cols.columns] = scaler.fit_transform(num_cols)

    df = pd.get_dummies(df)

    df.to_csv('/tmp/transformed.csv', index=False)












from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
# import sys
# sys.path.append("/opt/airflow")
# from scripts.extract import extract_from_minio
# from scripts.clean import clean_data
# from scripts.transform import transform_data
# from scripts.aggregate import aggregate_data
# from scripts.load import load_to_postgres

with DAG(
    'bigdata_pipeline',
    start_date=datetime(2025, 12, 15),
    schedule_interval=None,
    catchup=False
) as dag:

    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_from_minio
    )

    clean = PythonOperator(
        task_id='clean',
        python_callable=clean_data
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_data
    )

    aggregate = PythonOperator(
        task_id='aggregate',
        python_callable=aggregate_data
    )

    load = PythonOperator(
        task_id='load',
        python_callable=load_to_postgres
    )

    extract >> clean >> transform >> aggregate >> load
=======
#     extract = PythonOperator(
#         task_id='extract',
#         python_callable=extract_from_minio
#     )
#
#     clean = PythonOperator(
#         task_id='clean',
#         python_callable=clean_data
#     )
#
#     transform = PythonOperator(
#         task_id='transform',
#         python_callable=transform_data
#     )
#
#     aggregate = PythonOperator(
#         task_id='aggregate',
#         python_callable=aggregate_data
#     )
#
#     load = PythonOperator(
#         task_id='load',
#         python_callable=load_to_postgres
#     )
#
#     extract >> clean >> transform >> aggregate >> load
>>>>>>> 8f7e88994e8af728660a438e6f6ced615e4b67c2
