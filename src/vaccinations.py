import datetime as dt

import pandas as pd

from utils import db_helper as database
from config.core import config, config_db

RKI_DAILY_TRANSLATION = config.cols.rki_vaccinations_daily_cumulative['translation']
REPORTING_DATE = config.cols.rki_vaccinations_daily_cumulative['cols']['reporting_date']
BUNDESLAND_ID = config.cols.rki_vaccinations_daily_cumulative['cols']['bundesland_id']

GERMANY = config_db.cols['_countries']['countries']['germany'] # 'DE'
NUTS_0 = config_db.cols['_countries']['nuts_0']
AGEGROUP_INTERVAL_RKI = config_db.agegroup_intervals['rki']
GEO = config.cols.rki_covid_daily['cols']['geo']
RKI_DAILY_CUMULATIVE_TABLE = config_db.tables['vaccinations_daily_cumulative']

def rki_vaccinations_daily_cumulative(df: pd.DataFrame):
    db = database.ProjDB()
    tmp = df.copy()

    tmp.rename(columns=RKI_DAILY_TRANSLATION, inplace=True)

    if REPORTING_DATE in tmp.columns:
        try:
            tmp[REPORTING_DATE] = pd.to_datetime(tmp[REPORTING_DATE], infer_datetime_format=True).dt.date
            tmp[REPORTING_DATE] = pd.to_datetime(tmp[REPORTING_DATE], infer_datetime_format=True)
        except (KeyError, TypeError):
            print('Error trying to convert Date columns')

    tmp = tmp[tmp[BUNDESLAND_ID] == 0] # gesamt
    tmp[GEO] = GERMANY # init with germany as country
    tmp = db.merge_calendar_days_fk(df=tmp, left_on=REPORTING_DATE)
    tmp = db.merge_countries_fk(df=tmp, left_on=GEO, country_code=NUTS_0)
    db.insert_or_update(df=tmp, table=RKI_DAILY_CUMULATIVE_TABLE)
    db.db_close()
