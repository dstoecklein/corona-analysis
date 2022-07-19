import pandas as pd
from table_helpers import (
    calendars_helper as calh,
    agegroups_helper as aggh,
    countries_helper as counh,
    classifications_helper as clash,
    vaccines_helper as vach,
    populations_helper as poph,
)
from config import core as cfg
from config.core import cfg_init as init
from config.core import cfg_db, cfg_urls
from database.db import Database
from utils.get_data import estat

def init_calendars(database: Database) -> None:
    df_years = calh.create_calendar_year_df(init.from_config.calendar_start_year, init.from_config.calendar_end_year)
    df_weeks = calh.create_calendar_week_df(init.from_config.calendar_start_year, init.from_config.calendar_end_year)
    df_days = calh.create_calendar_day_df(init.from_config.calendar_start_year, init.from_config.calendar_end_year)

    database.upsert_df(df=df_years, table_name=cfg_db.tables.calendar_year)
    database.upsert_df(df=df_weeks, table_name=cfg_db.tables.calendar_week)
    database.upsert_df(df=df_days, table_name=cfg_db.tables.calendar_day)

def init_agegroups(database: Database) -> None:
    with database.ManagedSessionMaker() as session:
        aggh.insert_agegroups05y(session=session, agegroups=init.from_config.agegroups_05y)
        aggh.insert_agegroups10y(session=session, agegroups=init.from_config.agegroups_10y)
        aggh.insert_agegroupsRki(session=session, agegroups=init.from_config.agegroups_rki)

def init_classifications(database: Database) -> None:
    df_icd10 = pd.read_feather(cfg.get_files_path() / init.from_file.classifications_icd10.filename)
    df_icd10.set_index(init.from_file.classifications_icd10.index_col, inplace=True)
    icd10_dict = df_icd10.to_dict(orient="index")

    with database.ManagedSessionMaker() as session:
        clash.insert_icd10(session=session, icd10_dict=icd10_dict)

def init_countries(database: Database) -> None:
    # Countries
    df_countries = pd.read_feather(cfg.get_files_path() / init.from_file.countries.filename)
    df_countries.set_index(init.from_file.countries.index_col, inplace=True)
    countries_dict = df_countries.to_dict(orient="index")

    # Subdivisions Level 1
    df_subdivisions1 = pd.read_feather(cfg.get_files_path() / init.from_file.countries_subdivision1.filename)
    df_subdivisions1.set_index(init.from_file.countries_subdivision1.index_col, inplace=True)
    subdivisions1_dict = df_subdivisions1.to_dict(orient="index")

    # Subdivisions Level 2
    df_subdivisions2 = pd.read_feather(cfg.get_files_path() / init.from_file.countries_subdivision2.filename)
    df_subdivisions2.set_index(init.from_file.countries_subdivision2.index_col, inplace=True)
    subdivisions2_dict = df_subdivisions2.to_dict(orient="index")

    # Subdivisions Level 3
    df_subdivisions3 = pd.read_feather(cfg.get_files_path() / init.from_file.countries_subdivision3.filename)
    df_subdivisions3.set_index(init.from_file.countries_subdivision3.index_col, inplace=True)
    subdivisions3_dict = df_subdivisions3.to_dict(orient="index")

    with database.ManagedSessionMaker() as session:
        counh.insert_country(session=session, countries_dict=countries_dict)
        counh.insert_subdivision1(session=session, subdivisions1_dict=subdivisions1_dict)
        counh.insert_subdivision2(session=session, subdivisions2_dict=subdivisions2_dict)
        counh.insert_subdivision3(session=session, subdivisions3_dict=subdivisions3_dict)

def init_vaccines(database: Database) -> None:
    with database.ManagedSessionMaker() as session:
        vach.upsert_vaccine(session=session, vaccines=init.from_config.vaccines)
        vach.upsert_vaccine_series(session=session, vaccine_series=init.from_config.vaccine_series)

def init_population(database: Database) -> None:
    df = estat(
        code=cfg_urls.estat_population_annual_agegroup,
        purpose="POPULATION",
        save_file=False,
        data_type="csv",
        path=None,
    )
    with database.ManagedSessionMaker() as session:
        tmp_population = poph.transform_population_countries(session, df=df)
        database.upsert_df(df=tmp_population, table_name=cfg_db.tables.population_country)

        tmp_population_agegroups = poph.transform_population_countries_agegroups(session, df=df)
        database.upsert_df(df=tmp_population_agegroups, table_name=cfg_db.tables.population_country_agegroup)
        
        tmp_population_rki = poph.transform_population_countries_agegroups_rki(session, df=df)
        database.upsert_df(df=tmp_population_rki, table_name=cfg_db.tables.population_country_agegroup_rki)

def main(database: Database) -> None:
    init_calendars(database=database)
    init_agegroups(database=database)
    init_classifications(database=database)
    init_countries(database=database)
    init_vaccines(database=database)
    init_population(database=database)

if __name__ == "__main__":
    database = Database(db_uri=f"{cfg_db.db.dialect}{cfg_db.db.name}.db")
    main(database=database)