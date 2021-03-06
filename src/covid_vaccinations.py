import datetime as dt

import pandas as pd

from config.core import config, config_db
from utils import db_helper as database

OWID_DAILY_TRANSLATION = config.cols.owid_vaccinations_daily["translation"]
OWID_DAILY_MANUFACTURER_TRANSLATION = config.cols.owid_vaccinations_daily_manufacturer["translation"]
# RKI_DAILY_CUMULATIVE_TRANSLATION = config.cols.rki_vaccinations_daily_cumulative[
#    "translation"
# ]
RKI_DAILY_STATES_TRANSLATION = config.cols.rki_vaccinations_daily_states["translation"]
REPORTING_DATE = config.cols.rki_vaccinations_daily_cumulative["cols"]["reporting_date"]
BUNDESLAND_ID = config.cols.rki_vaccinations_daily_cumulative["cols"]["bundesland_id"]

GERMANY = config_db.cols["_countries"]["countries"]["germany"]  # 'DE'
NUTS_0 = config_db.cols["_countries"]["nuts_0"]
AGEGROUP_INTERVAL_RKI = config_db.agegroup_intervals["rki"]
GEO = config.cols.rki_covid_daily["cols"]["geo"]
ISO_3166_1_ALPHA3 = config.cols.owid_vaccinations_daily["cols"]["iso_3166_1_alpha3"]
LOCATION = config.cols.owid_vaccinations_daily_manufacturer["cols"]["location"]
RKI_DAILY_CUMULATIVE_TABLE = config_db.tables["vaccinations_daily_cumulative"]
RKI_DAILY_STATES_TABLE = config_db.tables["vaccinations_daily_states"]
OWID_DAILY_TABLE = config_db.tables["vaccinations_daily"]
OWID_DAILY_MANUFACTURER_TABLE = config_db.tables["vaccinations_daily_manufacturer"]
VACCINE = config.cols.rki_vaccinations_daily_states["cols"]["vaccine"]
VACC_DATE = config.cols.rki_vaccinations_daily_states["cols"]["vacc_date"]
VACCINE_SERIES = config.cols.rki_vaccinations_daily_states["cols"]["vaccine_series"]
SUBDIVISION_1_ID = config.cols.rki_vaccinations_daily_states["cols"]["subdivision_1_id"]
BRAND_NAME_1 = config.cols.owid_vaccinations_daily_manufacturer["cols"]["brand_name_1"]

def _convert_date(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    tmp = df.copy()

    if date_col in tmp.columns:
        try:
            tmp[date_col] = pd.to_datetime(
                tmp[date_col], infer_datetime_format=True
            ).dt.date
            tmp[date_col] = pd.to_datetime(tmp[date_col], infer_datetime_format=True)
        except (KeyError, TypeError):
            print("Error trying to convert Date columns")

    return tmp


def owid_vaccinations_daily(df: pd.DataFrame) -> None:
    db = database.DB()
    tmp = df.copy()

    tmp.rename(columns=OWID_DAILY_TRANSLATION, inplace=True)
    tmp = _convert_date(df=tmp, date_col=REPORTING_DATE)
    # only last 90days
    tmp = tmp[tmp[REPORTING_DATE] > dt.datetime.now() - pd.to_timedelta("90day")]
    tmp = db.merge_calendar_days_fk(df=tmp, left_on=REPORTING_DATE)
    tmp = db.merge_countries_fk(
        df=tmp, left_on=ISO_3166_1_ALPHA3, country_code=ISO_3166_1_ALPHA3
    )
    db.insert_or_update(df=tmp, table=OWID_DAILY_TABLE)
    db.db_close()


def owid_vaccinations_daily_manufacturer(df: pd.DataFrame):
    db = database.DB()
    tmp = df.copy()

    tmp.rename(columns=OWID_DAILY_MANUFACTURER_TRANSLATION, inplace=True)
    tmp = _convert_date(df=tmp, date_col=REPORTING_DATE)

    tmp[BRAND_NAME_1] = tmp[BRAND_NAME_1].str.lower()

    # map manufacturer to brand name
    manufacturer_to_brand_map = {
        'comirnaty': ['pfizer', 'biontech', 'pfizer-biontech', 'pfizer/biontech'],
        'janssen': ['johnson', 'johnson & johnson', 'johnson&johnson'], 
        'spikevax': ['moderna'],
        'vaxzevria': ['astrazeneca', 'astra', 'oxford/astrazeneca'],
        'sputnik v': ['gamaleya research institute of epidemiology', 'gamaleya', 'spuntik', 'sputnikv'],
        'sinopharm': ['beijing institute of biological products', 'sinopharm/beijing', 'sinopharm beijing', 'sinopharm_beijing'],
        'convidecia': ['cansino biologics', 'cansino'],
        'covaxin': ['bharat biotech', 'bharat'],
        'coronavac': ['sinovac'],
        'nuvaxovid': ['novavax', 'covovax'],
        'vidprevtyn': ['sanofi'],
    }

    for k, v in manufacturer_to_brand_map.items():
        tmp.replace(v, k, inplace=True)

    # only last 90days
    tmp = tmp[tmp[REPORTING_DATE] > dt.datetime.now() - pd.to_timedelta("90day")]
    tmp = db.merge_calendar_days_fk(df=tmp, left_on=REPORTING_DATE)
    tmp = db.merge_countries_fk(
        df=tmp, left_on=LOCATION, country_code='country_en'
    )
    tmp = db.merge_vaccines_fk(df=tmp, left_on=BRAND_NAME_1)
    db.insert_or_update(df=tmp, table=OWID_DAILY_MANUFACTURER_TABLE)
    db.db_close()

"""
def rki_vaccinations_daily_cumulative(df: pd.DataFrame) -> None:
    db = database.DB()
    tmp = df.copy()

    tmp.rename(columns=RKI_DAILY_CUMULATIVE_TRANSLATION, inplace=True)

    tmp = _convert_date(df=tmp, date_col=REPORTING_DATE)

    tmp = tmp[tmp[BUNDESLAND_ID] == 0]  # gesamt
    tmp[GEO] = GERMANY  # init with germany as country
    tmp = db.merge_calendar_days_fk(df=tmp, left_on=REPORTING_DATE)
    tmp = db.merge_countries_fk(df=tmp, left_on=GEO, country_code=NUTS_0)
    db.insert_or_update(df=tmp, table=RKI_DAILY_CUMULATIVE_TABLE)
    db.db_close()
"""


def rki_vaccinations_daily_states(df: pd.DataFrame) -> None:
    db = database.DB()
    tmp = df.copy()

    tmp.rename(columns=RKI_DAILY_STATES_TRANSLATION, inplace=True)

    tmp = _convert_date(df=tmp, date_col=VACC_DATE)

    tmp[VACCINE] = tmp[VACCINE].str.lower()

    # map manufacturer to brand name
    manufacturer_to_brand_map = {
        'comirnaty': ['pfizer', 'biontech', 'pfizer-biontech', 'pfizer/biontech'],
        'janssen': ['johnson', 'johnson & johnson', 'johnson&johnson'], 
        'spikevax': ['moderna'],
        'vaxzevria': ['astrazeneca', 'astra', 'oxford/astrazeneca'],
        'sputnik v': ['gamaleya research institute of epidemiology', 'gamaleya', 'spuntik', 'sputnikv'],
        'sinopharm': ['beijing institute of biological products', 'sinopharm/beijing', 'sinopharm beijing', 'sinopharm_beijing'],
        'convidecia': ['cansino biologics', 'cansino'],
        'covaxin': ['bharat biotech', 'bharat'],
        'coronavac': ['sinovac'],
        'nuvaxovid': ['novavax', 'covovax'],
        'vidprevtyn': ['sanofi'],
    }

    for k, v in manufacturer_to_brand_map.items():
        tmp.replace(v, k, inplace=True)

    # only last 90days
    tmp = tmp[tmp[VACC_DATE] > dt.datetime.now() - pd.to_timedelta("90day")]
    tmp = db.merge_vaccines_fk(df=tmp, left_on=VACCINE)
    tmp = db.merge_calendar_days_fk(df=tmp, left_on=VACC_DATE)
    tmp = db.merge_subdivisions_fk(
        df=tmp, left_on=SUBDIVISION_1_ID, level=1, subdiv_code=BUNDESLAND_ID
    )
    tmp = db.merge_vaccine_series_fk(df=tmp, left_on=VACCINE_SERIES)
    db.insert_or_update(df=tmp, table=RKI_DAILY_STATES_TABLE)
    db.db_close()
