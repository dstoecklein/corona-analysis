import pandas as pd
from src.database import db_helper as database
from src.utils import estat_helper


def population_countries(df: pd.DataFrame):
    db = database.ProjDB()
    tmp = df.copy()
    tmp = estat_helper.pre_process_population_states(tmp)
    tmp = tmp[tmp['level'] == 0]
    tmp = db.merge_calendar_years_fk(tmp, left_on='year')
    tmp = db.merge_countries_fk(tmp, left_on='geo', country_code='nuts_0')
    db.db_close()
    return tmp


def population_subdivision_1(df: pd.DataFrame):
    db = database.ProjDB()
    tmp = df.copy()
    tmp = estat_helper.pre_process_population_states(tmp)
    tmp = tmp[tmp['level'] == 1]
    tmp = db.merge_calendar_years_fk(tmp, left_on='year')
    tmp = db.merge_subdivisions_fk(tmp, left_on='geo', subdiv_code='nuts_1', level=1)
    tmp = tmp[tmp['country_subdivs_1_fk'].notna()]  # if no foreign key merged, then region is probably not available
    db.db_close()
    return tmp


def population_subdivision_2(df: pd.DataFrame):
    db = database.ProjDB()
    tmp = df.copy()
    tmp = estat_helper.pre_process_population_states(tmp)
    tmp = tmp[tmp['level'] == 2]
    tmp = db.merge_calendar_years_fk(tmp, left_on='year')
    tmp = db.merge_subdivisions_fk(tmp, left_on='geo', subdiv_code='nuts_2', level=2)
    tmp = tmp[tmp['country_subdivs_2_fk'].notna()]  # if no foreign key merged, then region is probably not available
    db.db_close()
    return tmp


def population_agegroups(df: pd.DataFrame):
    db = database.ProjDB()
    tmp = df.copy()
    tmp = estat_helper.pre_process_population_agegroups(tmp)
    tmp = db.merge_agegroups_fk(tmp, left_on='agegroup_10y', interval='10y')
    tmp = db.merge_calendar_years_fk(tmp, left_on='year')
    tmp = db.merge_countries_fk(tmp, left_on='geo', country_code='iso_3166_1_alpha2')
    db.db_close()
    return tmp


def life_exp_at_birth(df: pd.DataFrame):
    db = database.ProjDB()
    tmp = df.copy()
    tmp = estat_helper.pre_process_life_exp_at_birth(tmp)
    tmp = db.merge_calendar_years_fk(tmp, left_on='year')
    tmp = db.merge_countries_fk(tmp, left_on='geo', country_code='iso_3166_1_alpha2')
    db.db_close()
    return tmp


def median_age(df: pd.DataFrame):
    db = database.ProjDB()
    tmp = df.copy()
    tmp = estat_helper.pre_process_median_age(tmp)
    tmp = db.merge_calendar_years_fk(tmp, left_on='year')
    tmp = db.merge_countries_fk(tmp, left_on='geo', country_code='iso_3166_1_alpha2')
    db.db_close()
    return tmp
