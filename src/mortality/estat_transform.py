import eurostat
import pandas as pd
from src.database import db_helper as database
from src.utils import estat_helper


def deaths_weekly_agegroups(df: pd.DataFrame):
    db = database.ProjDB()
    tmp = df.copy()
    tmp = estat_helper.pre_process_deaths_weekly(tmp)
    tmp = db.merge_agegroups_fk(tmp, left_on='agegroup_10y', interval='10y')
    tmp = db.merge_calendar_weeks_fk(tmp, left_on='iso_key')
    tmp = db.merge_countries_fk(tmp, left_on='geo', country_code='iso_3166_1_alpha2')
    db.db_close()
    return tmp


def death_causes_annual_agegroups(df: pd.DataFrame):
    db = database.ProjDB()
    tmp = df.copy()
    tmp = estat_helper.pre_process(tmp)
    tmp = estat_helper.pre_process_death_causes_annual(tmp)
    tmp = db.merge_calendar_years_fk(tmp, left_on='year')
    tmp = db.merge_classifications_icd10_fk(tmp, left_on='icd10')
    tmp = db.merge_agegroups_fk(tmp, left_on='agegroup_10y', interval='10y')
    tmp = db.merge_countries_fk(tmp, left_on='geo', country_code='iso_3166_1_alpha2')

    tmp = tmp.groupby(
        [
            'classifications_icd10_fk',
            'agegroups_10y_fk',
            'countries_fk',
            'calendar_years_fk'
        ], as_index=False
    )['deaths'].sum()

    db.db_close()

    return tmp
