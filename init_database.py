from table_helpers import (
    calendar_helper as clh,
    agegroups_helper as agh,
    countries_helper as coh,
    icd10_helper as ich,
)
from config import core as cfg
from config.core import cfg_init as init
from config.core import cfg_db
from database.db import Database
import pandas as pd

def init_calendars(database: Database) -> None:
    with database.ManagedSessionMaker() as session:
        clh.add_new_calendar_years(
            session=session, 
            start_year=init.from_values.calendar_start_year,
            end_year=init.from_values.calendar_end_year
        )

def init_agegroups(database: Database) -> None:
    with database.ManagedSessionMaker() as session:
        agh.add_new_agegroups_05y(session=session, agegroups=init.from_values.agegroups_05y)
        agh.add_new_agegroups_10y(session=session, agegroups=init.from_values.agegroups_10y)
        agh.add_new_agegroups_rki(session=session, agegroups=init.from_values.agegroups_rki)

def init_icd10(database: Database) -> None:
    df = pd.read_feather(cfg.get_files_path() / init.from_files.icd10.filename)
    df.set_index(init.from_files.icd10.index_col, inplace=True)
    icd10_dict = df.to_dict(orient="index")
    
    with database.ManagedSessionMaker() as session:
        ich.add_new_icd10(session=session, icd10_dict=icd10_dict)

def init_countries(database: Database) -> None:
    # Countries
    df_countries = pd.read_feather(cfg.get_files_path() / init.from_files.countries.filename)
    df_countries.set_index(init.from_files.countries.index_col, inplace=True)
    countries_dict = df_countries.to_dict(orient="index")

    # Subdivisions Level 1
    df_subdivs1 = pd.read_feather(cfg.get_files_path() / init.from_files.subdivs1.filename)
    df_subdivs1.set_index(init.from_files.subdivs1.index_col, inplace=True)
    subdivs1_dict = df_subdivs1.to_dict(orient="index")

    # Subdivisions Level 2
    df_subdivs2 = pd.read_feather(cfg.get_files_path() / init.from_files.subdivs2.filename)
    df_subdivs2.set_index(init.from_files.subdivs2.index_col, inplace=True)
    subdivs2_dict = df_subdivs2.to_dict(orient="index")

    # Subdivisions Level 3
    df_subdivs3 = pd.read_feather(cfg.get_files_path() / init.from_files.subdivs3.filename)
    df_subdivs3.set_index(init.from_files.subdivs3.index_col, inplace=True)
    subdivs3_dict = df_subdivs3.to_dict(orient="index")

    with database.ManagedSessionMaker() as session:
        coh.add_new_country(session=session, countries_dict=countries_dict)
        coh.add_new_subdivision1(session=session, subdivs1_dict=subdivs1_dict)
        coh.add_new_subdivision2(session=session, subdivs2_dict=subdivs2_dict)
        coh.add_new_subdivision3(session=session, subdivs3_dict=subdivs3_dict)

def main(database: Database) -> None:
    init_calendars(database=database)
    init_agegroups(database=database)
    init_icd10(database=database)
    init_countries(database=database)

if __name__ == "__main__":
    database = Database(db_uri=f"{cfg_db.db.dialect}{cfg_db.db.name}.db")
    main(database=database)