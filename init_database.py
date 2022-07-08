from table_helpers import (
    calendars_helper as calh,
    agegroups_helper as aggh,
    countries_helper as counh,
    classifications_helper as clash,
)
from config import core as cfg
from config.core import cfg_init as init
from config.core import cfg_db
from database.db import Database
import pandas as pd

def init_calendars(database: Database) -> None:
    with database.ManagedSessionMaker() as session:
        calh.insert_calendar_years(
            session=session, 
            start_year=init.from_config.calendar_start_year,
            end_year=init.from_config.calendar_end_year
        )

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

def main(database: Database) -> None:
    init_calendars(database=database)
    init_agegroups(database=database)
    init_classifications(database=database)
    init_countries(database=database)

if __name__ == "__main__":
    database = Database(db_uri=f"{cfg_db.db.dialect}{cfg_db.db.name}.db")
    main(database=database)