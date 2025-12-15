import pandas as pd

def aggregate_data():
    df = pd.read_csv('/tmp/transformed.csv')

    agg = df.groupby(df.columns[0]).mean()

    agg.to_csv('/tmp/final.csv')
