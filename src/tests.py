import pandas as pd

from utils import db_helper as database
from config.core import config, config_db


RKI_WEEKLY_TRANSLATION = config.cols.rki_tests_weekly['translation']
RKI_WEEKLY_TABLE = config_db.tables['tests_weekly']
ISO_3166_1_ALPHA2 = config_db.cols['_countries']['iso_3166_1_alpha2']


def rki_weekly(df: pd.DataFrame) -> None:
    db = database.ProjDB()
    tmp = df.copy()

    tmp.rename(columns=RKI_WEEKLY_TRANSLATION, inplace=True)
    tmp = tmp[1:]  # delete first row
    tmp = tmp[:-1]  # delete last row
    # create ISO dates
    tmp[['iso_cw', 'iso_year']] = tmp['calendar_week'].str.split('/', expand=True)
    tmp['iso_cw'] = tmp['iso_cw'].str.zfill(2)
    tmp['iso_key'] = tmp['iso_year'] + tmp['iso_cw']
    tmp['iso_key'] = pd.to_numeric(tmp['iso_key'], errors='coerce')

    tmp = db.merge_calendar_weeks_fk(df=tmp, left_on='iso_key')
    tmp['geo'] = 'DE'
    tmp = db.merge_countries_fk(df=tmp, left_on='geo', country_code=ISO_3166_1_ALPHA2)    
    db.insert_or_update(df=tmp, table=RKI_WEEKLY_TABLE)
    db.db_close()
