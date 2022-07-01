import pandas as pd

from config.core import config, config_db
from utils import db_helper as database

GENESIS_HOSP_ANNUAL_TRANSLATION = config.cols.genesis_hospitals_annual["translation"]
GENESIS_HOSP_STAFF_ANNUAL_TRANSLATION = config.cols.genesis_hospitals_staff_annual[
    "translation"
]
ISO_YEAR = config.cols.genesis_hospitals_annual["cols"]["iso_year"]
GERMANY = config_db.cols["_countries"]["countries"]["germany"]  # 'DE'
GEO = config.cols.rki_covid_daily["cols"]["geo"]
ISO_3166_1_ALPHA2 = config_db.cols["_countries"]["iso_3166_1_alpha2"]
GENESIS_HOSP_ANNUAL_TABLE = config_db.tables["hospitals_annual"]
GENESIS_HOSP_STAFF_ANNUAL_TABLE = config_db.tables["hospitals_staff_annual"]


def genesis_hospitals_annual(df: pd.DataFrame) -> None:
    db = database.DB()
    tmp = df.copy()
    tmp.rename(columns=GENESIS_HOSP_ANNUAL_TRANSLATION, inplace=True)
    tmp[ISO_YEAR] = pd.to_datetime(
        tmp[ISO_YEAR], infer_datetime_format=True
    ).dt.year.astype(int)
    tmp = db.merge_calendar_years_fk(df=tmp, left_on=ISO_YEAR)
    tmp[GEO] = GERMANY
    tmp = db.merge_countries_fk(df=tmp, left_on=GEO, country_code=ISO_3166_1_ALPHA2)
    db.insert_or_update(df=tmp, table=GENESIS_HOSP_ANNUAL_TABLE)
    db.db_close()


def genesis_hospital_staff_annual(df: pd.DataFrame) -> None:
    db = database.DB()
    tmp = df.copy()
    tmp.to_csv("test.csv", sep=";")
    tmp.rename(columns=GENESIS_HOSP_STAFF_ANNUAL_TRANSLATION, inplace=True)
    tmp.to_csv("test2.csv", sep=";")
    tmp[ISO_YEAR] = pd.to_datetime(
        tmp[ISO_YEAR], infer_datetime_format=True
    ).dt.year.astype(int)
    tmp = db.merge_calendar_years_fk(df=tmp, left_on=ISO_YEAR)
    tmp[GEO] = GERMANY
    tmp = db.merge_countries_fk(df=tmp, left_on=GEO, country_code=ISO_3166_1_ALPHA2)
    db.insert_or_update(df=tmp, table=GENESIS_HOSP_STAFF_ANNUAL_TABLE)
    db.db_close()
