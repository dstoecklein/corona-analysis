import datetime as dt

import pandas as pd

from utils import db_helper as database


def rki_vaccinations_daily(config_cols: dict, config_db: dict, df: pd.DataFrame):
    db = database.ProjDB(config_db=config_db)
    tmp = df.copy()

    translation = config_cols['rki']['vaccinations_daily_github']['translation']
    tmp.rename(columns=translation, inplace=True)

    col_vacc_date = config_cols['rki']['vaccinations_daily_github']['col']['calendar_days']
    col_bundesland_id = config_cols['rki']['vaccinations_daily_github']['col']['bundesland_id']

    table = config_db['mysql']['tables']['vaccinations_daily_cumulative']

    if col_vacc_date in tmp.columns:
        try:
            tmp[col_vacc_date] = pd.to_datetime(tmp[col_vacc_date], infer_datetime_format=True).dt.date
            tmp[col_vacc_date] = pd.to_datetime(tmp[col_vacc_date], infer_datetime_format=True)
        except (KeyError, TypeError):
            print('Error trying to convert Date columns')

    tmp = tmp[tmp[col_bundesland_id] == 0] # gesamt
    tmp["GEO"] = "DE" # init with germany as country
    tmp = db.merge_calendar_days_fk(df=tmp, left_on=col_vacc_date)
    tmp = db.merge_countries_fk(df=tmp, left_on="GEO", country_code="nuts_0")
    db.insert_or_update(df=tmp, table=table)
    db.db_close()
