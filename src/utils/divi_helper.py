import pandas as pd


def pre_process(df: pd.DataFrame):
    tmp = df.copy()

    if 'date' in tmp.columns:
        try:
            tmp['date'] = pd.to_datetime(tmp['date'], infer_datetime_format=True).dt.date
            tmp['date'] = pd.to_datetime(tmp['date'], infer_datetime_format=True)
        except (KeyError, TypeError):
            print('Error trying to convert Date columns')

    if 'Datum' in tmp.columns:
        try:
            tmp['Datum'] = pd.to_datetime(tmp['Datum'], infer_datetime_format=True, utc=True).dt.date
            tmp['Datum'] = pd.to_datetime(tmp['Datum'], infer_datetime_format=True)
        except (KeyError, TypeError):
            print('Error trying to convert Date columns')

    return tmp
