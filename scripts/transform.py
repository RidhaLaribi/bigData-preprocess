from sklearn.preprocessing import StandardScaler
import pandas as pd

def transform_data():
    df = pd.read_csv('/tmp/clean.csv')

    num_cols = df.select_dtypes(include='number')
    scaler = StandardScaler()
    df[num_cols.columns] = scaler.fit_transform(num_cols)

    df = pd.get_dummies(df)

    df.to_csv('/tmp/transformed.csv', index=False)
