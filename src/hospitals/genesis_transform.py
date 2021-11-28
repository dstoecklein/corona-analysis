import pandas as pd
from src.database import db_helper as database
from src.utils import genesis_helper


def hospitals_annual(df: pd.DataFrame):
    db = database.ProjDB()
    tmp = df.copy()
    tmp = genesis_helper.pre_process_hospitals_annual(tmp)
    tmp = db.merge_calendar_years_fk(df=tmp, left_on='iso_year')
    tmp['geo'] = 'DE'
    tmp = db.merge_countries_fk(df=tmp, left_on='geo', country_code='iso_3166_1_alpha2')
    db.db_close()
    return tmp


def hospital_staff_annual(df: pd.DataFrame):
    db = database.ProjDB()
    tmp = df.copy()
    tmp = genesis_helper.pre_process_hospital_staff_annual(tmp)
    tmp['iso_year'] = pd.to_datetime(tmp['iso_year'], infer_datetime_format=True).dt.year
    tmp = db.merge_calendar_years_fk(df=tmp, left_on='iso_year')
    tmp['geo'] = 'DE'
    tmp = db.merge_countries_fk(df=tmp, left_on='geo', country_code='iso_3166_1_alpha2')
    db.db_close()
    return tmp

