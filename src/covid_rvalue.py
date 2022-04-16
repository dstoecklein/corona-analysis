import pandas as pd

from config.core import config, config_db
from utils import db_helper as database

RKI_DAILY_TRANSLATION = config.cols.rki_rvalue_daily["translation"]
RKI_DAILY_TABLE = config_db.tables["rvalue_daily"]
ISO_3166_1_ALPHA2 = config_db.cols["_countries"]["iso_3166_1_alpha2"]
DATE = config.cols.rki_rvalue_daily["cols"]["date"]
GERMANY = config_db.cols["_countries"]["countries"]["germany"]  # 'DE'
GEO = config.cols.rki_rvalue_daily["cols"]["geo"]


def rki_daily(df: pd.DataFrame) -> None:
    db = database.ProjDB()
    tmp = df.copy()

    tmp.rename(columns=RKI_DAILY_TRANSLATION, inplace=True)
    tmp = tmp.fillna(0)
    tmp[DATE] = pd.to_datetime(tmp[DATE])

    tmp = db.merge_calendar_days_fk(df=tmp, left_on=DATE)
    tmp[GEO] = GERMANY
    tmp = db.merge_countries_fk(df=tmp, left_on=GEO, country_code=ISO_3166_1_ALPHA2)
    db.insert_or_update(df=tmp, table=RKI_DAILY_TABLE)
    db.db_close()
