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

def rki_daily(df: pd.DataFrame, date: dt.datetime = TODAY) -> None:
    db = database.ProjDB()
    
    tmp = df.copy()
    tmp.rename(columns=RKI_DAILY_TRANSLATION, inplace=True)
    tmp = covid_helper.rki_pre_process(df=tmp)
    tmp = covid_helper.rki_calc_numbers(df=tmp, date=date)
    tmp = tmp.groupby('reporting_date').sum().reset_index()
    tmp['geo'] = 'DE'
    tmp = covid_helper.rki_calc_7d_incidence(
        config_cols=config.cols,
        config_db=config_db,
        df=tmp,
        level=0,
        reference_year=INCIDENCE_REF_YEAR
    )
    tmp = db.merge_calendar_days_fk(df=tmp, left_on='reporting_date')
    tmp.to_csv("Test.csv", sep=";")
    #db.insert_or_update(df=tmp, table=RKI_DAILY_TABLE)
    #db.db_close()


def rki_daily_states(df: pd.DataFrame, date: dt.datetime = TODAY) -> None:
    db = database.ProjDB()
    tmp = df.copy()
    tmp.rename(columns=RKI_DAILY_TRANSLATION, inplace=True)
    tmp = covid_helper.rki_pre_process(df=tmp)
    tmp = covid_helper.rki_calc_numbers(df=tmp, date=date)
    tmp = tmp[tmp['subdivision_1_id'] > 0]  # ignore rows with IdBundesland -1 (nicht erhoben)
    tmp = tmp.groupby(['subdivision_1_id', 'reporting_date']).sum().reset_index()
    tmp = covid_helper.rki_calc_7d_incidence(
        config_cols=config.cols,
        config_db=config_db,
        df=tmp,
        level=1,
        reference_year=INCIDENCE_REF_YEAR
    )
    tmp = db.merge_calendar_days_fk(df=tmp, left_on='reporting_date')
    #db.insert_or_update(df=tmp, table=RKI_DAILY_STATES_TABLE)
    #db.db_close()