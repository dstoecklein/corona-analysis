import datetime as dt

import pandas as pd

from utils import db_helper as database
from config.core import config, config_db

RKI_DAILY_CUMULATIVE_TRANSLATION = config.cols.rki_vaccinations_daily_cumulative['translation']
RKI_DAILY_STATES_TRANSLATION = config.cols.rki_vaccinations_daily_states['translation']
REPORTING_DATE = config.cols.rki_vaccinations_daily_cumulative['cols']['reporting_date']
BUNDESLAND_ID = config.cols.rki_vaccinations_daily_cumulative['cols']['bundesland_id']

GERMANY = config_db.cols['_countries']['countries']['germany'] # 'DE'
NUTS_0 = config_db.cols['_countries']['nuts_0']
AGEGROUP_INTERVAL_RKI = config_db.agegroup_intervals['rki']
GEO = config.cols.rki_covid_daily['cols']['geo']
RKI_DAILY_CUMULATIVE_TABLE = config_db.tables['vaccinations_daily_cumulative']
RKI_DAILY_STATES_TABLE = config_db.tables['vaccinations_daily_states']
VACCINE = config.cols.rki_vaccinations_daily_states['cols']['vaccine']
VACC_DATE = config.cols.rki_vaccinations_daily_states['cols']['vacc_date']
VACCINE_SERIES = config.cols.rki_vaccinations_daily_states['cols']['vaccine_series']
SUBDIVISION_1_ID = config.cols.rki_vaccinations_daily_states['cols']['subdivision_1_id']


def _rki_daily_pp(df: pd.DataFrame, date_col: str) -> pd.DataFrame: # pre processor for daily RKI vacc data
    tmp = df.copy()

    if date_col in tmp.columns:
        try:
            tmp[date_col] = pd.to_datetime(tmp[date_col], infer_datetime_format=True).dt.date
            tmp[date_col] = pd.to_datetime(tmp[date_col], infer_datetime_format=True)
        except (KeyError, TypeError):
            print('Error trying to convert Date columns')

    return tmp


def rki_vaccinations_daily_cumulative(df: pd.DataFrame) -> None:
    db = database.ProjDB()
    tmp = df.copy()

    tmp.rename(columns=RKI_DAILY_CUMULATIVE_TRANSLATION, inplace=True)

    tmp = _rki_daily_pp(df=tmp, date_col=REPORTING_DATE)

    tmp = tmp[tmp[BUNDESLAND_ID] == 0] # gesamt
    tmp[GEO] = GERMANY # init with germany as country
    tmp = db.merge_calendar_days_fk(df=tmp, left_on=REPORTING_DATE)
    tmp = db.merge_countries_fk(df=tmp, left_on=GEO, country_code=NUTS_0)
    db.insert_or_update(df=tmp, table=RKI_DAILY_CUMULATIVE_TABLE)
    db.db_close()


def rki_vaccinations_daily_states(df: pd.DataFrame) -> None:
    db = database.ProjDB()
    tmp = df.copy()

    tmp.rename(columns=RKI_DAILY_STATES_TRANSLATION, inplace=True)

    tmp = _rki_daily_pp(df=tmp, date_col=VACC_DATE)

    tmp = db.merge_vaccines_fk(df=tmp, left_on=VACCINE)
    tmp = db.merge_calendar_days_fk(df=tmp, left_on=VACC_DATE)
    tmp = db.merge_subdivisions_fk(df=tmp, left_on=SUBDIVISION_1_ID, level=1, subdiv_code=BUNDESLAND_ID)
    tmp = db.merge_vaccine_series_fk(df=tmp, left_on=VACCINE_SERIES)
    db.insert_or_update(df=tmp, table=RKI_DAILY_STATES_TABLE)
    db.db_close()
 