import datetime as dt

import pandas as pd

from config.core import config, config_db
from utils import db_helper as database

DIVI_DAILY_COUNTIES_TRANSLATION = config.cols.divi_itcu_daily_counties["translation"]
DIVI_DAILY_STATES_TRANSLATION = config.cols.divi_itcu_daily_states["translation"]
REPORTING_DATE = config.cols.divi_itcu_daily_counties["cols"]["reporting_date"]
AGS = config.cols.divi_itcu_daily_counties["cols"]["ags"]
DIVI_DAILY_COUNTIES_TABLE = config_db.tables["itcu_daily_counties"]
DIVI_DAILY_STATES_TABLE = config_db.tables["itcu_daily_states"]
SUBDIVISION_1 = config.cols.divi_itcu_daily_states["cols"]["subdivision_1"]
CASES_COVID_INITIAL_RECEPTION = config.cols.divi_itcu_daily_states["cols"][
    "cases_covid_initial_reception"
]


def _convert_date(df: pd.DataFrame, date_col: str, utc: bool) -> pd.DataFrame:
    tmp = df.copy()

    if date_col in tmp.columns:
        try:
            tmp[date_col] = pd.to_datetime(
                tmp[date_col], infer_datetime_format=True, utc=utc
            ).dt.date
            tmp[date_col] = pd.to_datetime(tmp[date_col], infer_datetime_format=True)
        except (KeyError, TypeError):
            print("Error trying to convert Date columns")
    return tmp


def divi_daily_counties(df: pd.DataFrame) -> None:
    db = database.DB()
    tmp = df.copy()
    tmp.rename(columns=DIVI_DAILY_COUNTIES_TRANSLATION, inplace=True)
    tmp = _convert_date(df=tmp, date_col=REPORTING_DATE, utc=False)
    tmp = tmp[
        tmp[REPORTING_DATE] > dt.datetime.now() - pd.to_timedelta("90day")
    ]  # only get last 90 days
    tmp = db.merge_subdivisions_fk(df=tmp, left_on=AGS, level=3, subdiv_code=AGS)
    tmp = db.merge_calendar_days_fk(df=tmp, left_on=REPORTING_DATE)
    db.insert_or_update(df=tmp, table=DIVI_DAILY_COUNTIES_TABLE)
    db.db_close()


def divi_daily_states(df: pd.DataFrame) -> None:
    db = database.DB()
    tmp = df.copy()
    tmp.rename(columns=DIVI_DAILY_STATES_TRANSLATION, inplace=True)
    tmp = _convert_date(df=tmp, date_col=REPORTING_DATE, utc=True)
    tmp[SUBDIVISION_1] = tmp[SUBDIVISION_1].str.lower()
    tmp[SUBDIVISION_1] = tmp[SUBDIVISION_1].str.replace("ae", "ä")
    tmp[SUBDIVISION_1] = tmp[SUBDIVISION_1].str.replace("ue", "ü")
    tmp[SUBDIVISION_1] = tmp[SUBDIVISION_1].str.replace("oe", "ö")
    tmp[SUBDIVISION_1] = tmp[SUBDIVISION_1].str.replace("_", "-")
    tmp[SUBDIVISION_1] = tmp[SUBDIVISION_1].astype(str)
    tmp = db.merge_subdivisions_fk(
        df=tmp, left_on=SUBDIVISION_1, level=1, subdiv_code=SUBDIVISION_1
    )
    tmp = db.merge_calendar_days_fk(df=tmp, left_on=REPORTING_DATE)
    tmp[CASES_COVID_INITIAL_RECEPTION] = tmp[CASES_COVID_INITIAL_RECEPTION].fillna(0)
    db.insert_or_update(df=tmp, table=DIVI_DAILY_STATES_TABLE)
    db.db_close()
