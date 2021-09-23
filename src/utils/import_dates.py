import pandas as pd
from datetime import datetime
from src.mysql_db import db_helper as database

prj_db = database.ProjDB()


def get_cw_data(df: pd.DataFrame):
    # create ISO dates and reformat date
    df['iso_key'] = df['datum'].dt.strftime('%G%V')
    df['iso_key'] = pd.to_numeric(df['iso_key'], errors='coerce')

    # get foreign keys
    df_calendar_cw = pd.read_sql("SELECT * FROM calendar_cw", prj_db.connection)
    df = pd.merge(df, df_calendar_cw, left_on='iso_key', right_on='iso_key', how='left') \
        .drop('iso_key', axis=1) \
        .drop('calendar_yr_id', axis=1) \
        .drop('iso_cw', axis=1)

    df = df.fillna(53)
    df['calendar_cw_id'] = df['calendar_cw_id'].astype(int)
    return df


def get_calendar_data(df: pd.DataFrame, col_name: str):
    # create ISO dates and reformat date
    a = df[col_name]
    df[col_name] = pd.to_datetime(df[col_name], utc=True)

    df = df.assign(**{col_name: df[col_name].dt.strftime('%Y-%m-%d')})

    # get foreign keys
    df_calendar_dt = pd.read_sql("SELECT * FROM calendar_dt", prj_db.connection)
    df[col_name] = df[col_name].astype(str)
    df_calendar_dt['datum'] = df_calendar_dt['datum'].astype(str)

    df = pd.merge(df, df_calendar_dt, left_on=col_name, right_on='datum', how='left')
    df = df.drop(['calendar_cw_id'], axis=1)
    if col_name != 'datum':
        df = df.drop(['datum'], axis=1)
    df[col_name] = a
    return df


def import_dates_to_db():
    startdate = pd.to_datetime(datetime.strptime('04.01.2010', '%d.%m.%Y'), utc=True)
    enddate = pd.to_datetime(datetime.strptime('31.12.2025', '%d.%m.%Y'), utc=True)

    range = pd.date_range(startdate, enddate)
    df_range = pd.DataFrame({'datum': range})
    df_range = get_cw_data(df_range)
    prj_db.insert_and_append(df_range, 'calendar_dt')
    print('done')
