import pandas as pd

from config.core import config, config_db
from utils import db_helper as database

RKI_WEEKLY_TRANSLATION = config.cols.rki_tests_weekly["translation"]
ISO_CW = config.cols.rki_tests_weekly["cols"]["iso_cw"]
ISO_YEAR = config.cols.rki_tests_weekly["cols"]["iso_year"]
ISO_KEY = config.cols.rki_tests_weekly["cols"]["iso_key"]
CALENDAR_WEEK = config.cols.rki_tests_weekly["cols"]["calendar_week"]
RKI_WEEKLY_TABLE = config_db.tables["tests_weekly"]
ISO_3166_1_ALPHA2 = config_db.cols["_countries"]["iso_3166_1_alpha2"]
GERMANY = config_db.cols["_countries"]["countries"]["germany"]  # 'DE'
GEO = config.cols.rki_tests_weekly["cols"]["geo"]


def rki_weekly(df: pd.DataFrame) -> None:
    db = database.DB()
    tmp = df.copy()

    tmp.rename(columns=RKI_WEEKLY_TRANSLATION, inplace=True)
    tmp = tmp[1:]  # delete first row
    tmp = tmp[:-1]  # delete last row
    # create ISO dates
    tmp[[ISO_CW, ISO_YEAR]] = tmp[CALENDAR_WEEK].str.split("/", expand=True)
    tmp[ISO_CW] = tmp[ISO_CW].str.zfill(2)
    tmp[ISO_KEY] = tmp[ISO_YEAR] + tmp[ISO_CW]
    tmp[ISO_KEY] = pd.to_numeric(tmp[ISO_KEY], errors="coerce")

    tmp = db.merge_calendar_weeks_fk(df=tmp, left_on=ISO_KEY)
    tmp[GEO] = GERMANY
    tmp = db.merge_countries_fk(df=tmp, left_on=GEO, country_code=ISO_3166_1_ALPHA2)
    db.insert_or_update(df=tmp, table=RKI_WEEKLY_TABLE)
    db.db_close()
