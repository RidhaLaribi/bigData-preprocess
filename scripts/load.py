import pandas as pd
from sqlalchemy import create_engine

def load_to_postgres():
    engine = create_engine(
        'postgresql://airflow:airflow@postgres:5432/analytics'
    )

    df = pd.read_csv('/tmp/final.csv')
    df.to_sql('final_table', engine, if_exists='replace', index=False)
