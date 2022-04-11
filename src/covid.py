import datetime as dt

import pandas as pd

from utils import covid_helper
from utils import db_helper as database
from config.core import config, config_db

# Constants
TODAY = dt.date.today()
TODAY = dt.datetime(TODAY.year, TODAY.month, TODAY.day)
INCIDENCE_REF_YEAR = config.data.incidence_reference_year
RKI_DAILY_TRANSLATION = config.cols.rki_covid_daily['translation']
RKI_DAILY_TABLE = config_db.tables['covid_daily']
RKI_DAILY_STATES_TABLE = config_db.tables['covid_daily_states']
SUBDIVISION_2_ID = config.cols.rki_covid_daily['cols']['subdivision_2_id']
REPORTING_DATE = config.cols.rki_covid_daily['cols']['reporting_date']
BUNDESLAND_ID = config.cols.rki_covid_daily['cols']['bundesland_id']


def rki_daily(df: pd.DataFrame, date: dt.datetime = TODAY) -> None:
    db = database.ProjDB()
    
    tmp = df.copy()
    tmp.rename(columns=RKI_DAILY_TRANSLATION, inplace=True)
    tmp = covid_helper.rki_pre_process(df=tmp)
    tmp = covid_helper.rki_calc_numbers(df=tmp, date=date)
    tmp = tmp.groupby(REPORTING_DATE).sum().reset_index()

    tmp['geo'] = 'DE'
    tmp = covid_helper.rki_calc_7d_incidence(
        df=tmp,
        level=0,
        reference_year=INCIDENCE_REF_YEAR
    )
    tmp = db.merge_calendar_days_fk(df=tmp, left_on=REPORTING_DATE)
    db.insert_or_update(df=tmp, table=RKI_DAILY_TABLE)
    db.db_close()


def rki_daily_states(df: pd.DataFrame, date: dt.datetime = TODAY) -> None:
    db = database.ProjDB()
    tmp = df.copy()
    tmp.rename(columns=RKI_DAILY_TRANSLATION, inplace=True)
    tmp = covid_helper.rki_pre_process(df=tmp)
    tmp = covid_helper.rki_calc_numbers(df=tmp, date=date)

    tmp = tmp[tmp[BUNDESLAND_ID] > 0]  # ignore rows with IdBundesland -1 (nicht erhoben)
    tmp = tmp.groupby([BUNDESLAND_ID, REPORTING_DATE]).sum().reset_index()
    tmp = covid_helper.rki_calc_7d_incidence(
        df=tmp,
        level=1,
        reference_year=INCIDENCE_REF_YEAR
    )
    tmp = db.merge_calendar_days_fk(df=tmp, left_on=REPORTING_DATE)
    db.insert_or_update(df=tmp, table=RKI_DAILY_STATES_TABLE)
    db.db_close()


def rki_daily_counties(df: pd.DataFrame, date: dt.datetime = TODAY) -> None:
    db = database.ProjDB()
    tmp = df.copy()
    tmp.rename(columns=RKI_DAILY_TRANSLATION, inplace=True)
    tmp = covid_helper.rki_pre_process(df=tmp)
    tmp = covid_helper.rki_calc_numbers(df=tmp, date=date)

    tmp = tmp[tmp[BUNDESLAND_ID] > 0]  # ignore rows with IdBundesland -1 (nicht erhoben)

    tmp[SUBDIVISION_2_ID] = tmp[SUBDIVISION_2_ID].astype(int).replace({ # combin berlin districts
        11001: 11000,
        11002: 11000,
        11003: 11000,
        11004: 11000,
        11005: 11000,
        11006: 11000,
        11007: 11000,
        11008: 11000,
        11009: 11000,
        11010: 11000,
        11011: 11000,
        11012: 11000
    })

    tmp = tmp.groupby([SUBDIVISION_2_ID, REPORTING_DATE]).sum().reset_index()
    tmp = covid_helper.rki_calc_7d_incidence(
        df=tmp,
        level=3,
        reference_year='2021'
    )
    tmp = db.merge_calendar_days_fk(df=tmp, left_on=REPORTING_DATE)
    tmp.to_csv("test.csv", index=False, sep=";")
    #db.insert_or_update(df=tmp, table=RKI_DAILY_STATES_TABLE)
    #db.db_close()