import pandas as pd


def create_iso_key(df: pd.DataFrame, column_name: str):
    tmp = df.copy()
    tmp = tmp.assign(iso_key=tmp[column_name].dt.strftime('%G%V').astype(int))
    return tmp
