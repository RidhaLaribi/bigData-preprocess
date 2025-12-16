
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
# from scripts.extract import extract_from_minio
# from scripts.clean import clean_data
# from scripts.transform import transform_data
# from scripts.aggregate import aggregate_data
# from scripts.load import load_to_postgres
#
# with DAG(
#     'bigdata_pipeline',
#     start_date=datetime(2025, 12, 15),
#     schedule_interval=None,
#     catchup=False
# ) as dag:
#
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