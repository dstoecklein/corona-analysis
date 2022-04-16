import datetime as dt

import pandas as pd

from utils import calculation_helper
from utils import db_helper as database
from config.core import config, config_db

# Constants
TODAY = dt.date.today()
TODAY = dt.datetime(TODAY.year, TODAY.month, TODAY.day)
INCIDENCE_REF_YEAR = config.data.incidence_reference_year
RKI_DAILY_TRANSLATION = config.cols.rki_covid_daily['translation']
RKI_DAILY_TABLE = config_db.tables['covid_daily']
RKI_DAILY_STATES_TABLE = config_db.tables['covid_daily_states']
RKI_DAILY_COUNTIES_TABLE = config_db.tables['covid_daily_counties']
RKI_DAILY_AGEGROUPS_TABLE = config_db.tables['covid_daily_agegroups']
RKI_WEEKLY_CUMULATIVE = config_db.tables['covid_weekly_cumulative']
SUBDIVISION_2_ID = config.cols.rki_covid_daily['cols']['subdivision_2_id']
REPORTING_DATE = config.cols.rki_covid_daily['cols']['reporting_date']
BUNDESLAND_ID = config.cols.rki_covid_daily['cols']['bundesland_id']
SEX = config.cols.rki_covid_daily['cols']['sex']
RKI_AGEGROUPS = config.cols.rki_covid_daily['cols']['rki_agegroups']
RKI_AGEGROUP_MAPPING = config.cols.rki_covid_daily['agegroup_mapping']
BERLIN_DISTRICT_MAPPING = config.cols.rki_covid_daily['berlin_district_mapping']
REFERENCE_DATE = config.cols.rki_covid_daily["cols"]['reference_date']
GERMANY = config_db.cols['_countries']['countries']['germany'] # 'DE'
NUTS_0 = config_db.cols['_countries']['nuts_0']
AGEGROUP_INTERVAL_RKI = config_db.agegroup_intervals['rki']
GEO = config.cols.rki_covid_daily['cols']['geo']
ISO_KEY = config.cols.rki_covid_daily['cols']['iso_key']


def _convert_date(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    tmp = df.copy()

    tmp.rename(columns=RKI_DAILY_TRANSLATION, inplace=True)

    if REPORTING_DATE in tmp.columns:
        try:
            tmp[REPORTING_DATE] = pd.to_datetime(tmp[REPORTING_DATE], infer_datetime_format=True).dt.date
            tmp[REPORTING_DATE] = pd.to_datetime(tmp[REPORTING_DATE], infer_datetime_format=True)
        except (KeyError, TypeError):
            print('Error trying to convert Date columns')
    if REFERENCE_DATE in tmp.columns:
        try:
            tmp[REFERENCE_DATE] = pd.to_datetime(tmp[REFERENCE_DATE], infer_datetime_format=True).dt.date
            tmp[REFERENCE_DATE] = pd.to_datetime(tmp[REFERENCE_DATE], infer_datetime_format=True)
        except (KeyError, TypeError):
            print('Error trying to convert Date columns')

    return tmp


def rki_daily(df: pd.DataFrame, date: dt.datetime = TODAY) -> None:
    db = database.ProjDB()
    tmp = df.copy()
 
    tmp = _convert_date(df=tmp, date_col=REPORTING_DATE)
    tmp = calculation_helper.rki_calc_numbers(df=tmp, date=date)
    tmp = tmp.groupby(REPORTING_DATE).sum().reset_index()
    tmp[GEO] = GERMANY
    tmp = calculation_helper.rki_calc_7d_incidence(
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

    tmp = _convert_date(df=tmp, date_col=REPORTING_DATE)
    tmp = calculation_helper.rki_calc_numbers(df=tmp, date=date)
    tmp = tmp[tmp[BUNDESLAND_ID] > 0]  # ignore rows with IdBundesland -1 (nicht erhoben)
    tmp = tmp.groupby([BUNDESLAND_ID, REPORTING_DATE]).sum().reset_index()
    tmp = calculation_helper.rki_calc_7d_incidence(
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

    tmp = _convert_date(df=tmp, date_col=REPORTING_DATE)
    tmp = calculation_helper.rki_calc_numbers(df=tmp, date=date)
    tmp = tmp[tmp[BUNDESLAND_ID] > 0]  # ignore rows with IdBundesland -1 (nicht erhoben)
    tmp[SUBDIVISION_2_ID] = tmp[SUBDIVISION_2_ID].astype(int).replace(BERLIN_DISTRICT_MAPPING) # combine berlin district
    tmp = tmp.groupby([SUBDIVISION_2_ID, REPORTING_DATE]).sum().reset_index()
    tmp = calculation_helper.rki_calc_7d_incidence(
        df=tmp,
        level=3,
        reference_year='2021'
    )

    tmp = db.merge_calendar_days_fk(df=tmp, left_on=REPORTING_DATE)
    db.insert_or_update(df=tmp, table=RKI_DAILY_COUNTIES_TABLE)
    db.db_close()


def rki_daily_agegroups(df: pd.DataFrame, date: dt.datetime = TODAY) -> None:
    db = database.ProjDB()
    tmp = df.copy()

    tmp = _convert_date(df=tmp, date_col=REPORTING_DATE)
    tmp = tmp[tmp[SEX] != 'unbekannt']
    tmp = calculation_helper.rki_calc_numbers(df=tmp, date=date)
    tmp = tmp.groupby([RKI_AGEGROUPS, REPORTING_DATE]).sum().reset_index()
    tmp.replace({
        RKI_AGEGROUPS: RKI_AGEGROUP_MAPPING
    }, inplace=True)
    tmp[GEO] = GERMANY
    tmp = calculation_helper.rki_calc_7d_incidence(
        df=tmp,
        level=0,
        reference_year=INCIDENCE_REF_YEAR
    )

    tmp = db.merge_calendar_days_fk(df=tmp, left_on=REPORTING_DATE)
    tmp = db.merge_agegroups_fk(df=tmp, left_on=RKI_AGEGROUPS, interval=AGEGROUP_INTERVAL_RKI)
    db.insert_or_update(df=tmp, table=RKI_DAILY_AGEGROUPS_TABLE)
    db.db_close()


def rki_weekly_cumulative(df: pd.DataFrame) -> None:
    db = database.ProjDB()
    tmp = df.copy()
    tmp = _convert_date(df=tmp, date_col=REPORTING_DATE)
    tmp = calculation_helper.rki_calc_numbers(df=tmp, date=tmp[REPORTING_DATE])
    tmp = tmp.assign(iso_key=tmp[REPORTING_DATE].dt.strftime('%G%V').astype(int))
    tmp = tmp.groupby([ISO_KEY]).sum().reset_index()
    tmp[GEO] = GERMANY

    tmp = db.merge_calendar_weeks_fk(df=tmp, left_on=ISO_KEY)
    tmp = db.merge_countries_fk(df=tmp, left_on=GEO, country_code=NUTS_0)
    db.insert_or_update(df=tmp, table=RKI_WEEKLY_CUMULATIVE)
    db.db_close()
