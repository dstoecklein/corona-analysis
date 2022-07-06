import table_helpers.agegroups_helper as ah
import table_helpers.calendar_helper as ch
import table_helpers.icd10_helper as icd10
from config.core import cfg_init_values as init
from config.core import cfg_table_names as tbl
from config.core import cfg_db
from database.db import Database
import pandas as pd

def init_calendars(db: Database) -> None:
    df_years = ch.create_calendar_years_df(init.calendar_start_year, init.calendar_end_year)
    df_weeks = ch.create_calendar_weeks_df(init.calendar_start_year, init.calendar_end_year)
    df_days = ch.create_calendar_days_df(init.calendar_start_year, init.calendar_end_year)

    db.upsert_df(df=df_years, table_name=tbl.calendar_years)
    db.upsert_df(df=df_weeks, table_name=tbl.calendar_weeks)
    db.upsert_df(df=df_days, table_name=tbl.calendar_days)

def init_agegroups(db: Database) -> None:
    with db.ManagedSessionMaker() as session:
        with session.begin():
            ah.add_new_agegroups_05y(session=session, agegroups=init.agegroups_05y)
            ah.add_new_agegroups_10y(session=session, agegroups=init.agegroups_10y)
            ah.add_new_agegroups_rki(session=session, agegroups=init.agegroups_rki)

def init_icd10(db: Database):
    df = pd.read_feather("files/icd10.ftr") # TODO: add to config
    df.set_index("icd10", inplace=True)
    icd10_dict = df.to_dict(orient="index")
    with db.ManagedSessionMaker() as session:
        with session.begin():
            icd10.add_new_icd10(session=session, icd10_dict=icd10_dict)


def main(db: Database):
    init_calendars(db=db)
    init_agegroups(db=db)
    init_icd10(db=db)

if __name__ == "__main__":
    db = Database(db_uri=f"{cfg_db.dialect}{cfg_db.name}.db")
    main(db=db)