import table_helpers.agegroups_helper as ah
import table_helpers.calendar_helper as ch
from config.core import cfg_init_values as init
from config.core import cfg_table_names as tbl
from config.core import cfg_db
from database.db import Database


def init_calendars(db: Database, start_year: int, end_year: int) -> None:
    df_years = ch.create_calendar_years_df(start_year, end_year)
    df_weeks = ch.create_calendar_weeks_df(start_year, end_year)
    df_days = ch.create_calendar_days_df(start_year, end_year)

    db.upsert_df(df=df_years, table_name=tbl.calendar_years)
    db.upsert_df(df=df_weeks, table_name=tbl.calendar_weeks)
    db.upsert_df(df=df_days, table_name=tbl.calendar_days)


def init_agegroups(
    db: Database, agegroups_05y: list, agegroups_10y: list, agegroups_rki: list
) -> None:
    with db.ManagedSessionMaker() as session:
        with session.begin():
            ah.add_new_agegroups_05y(session=session, agegroups=agegroups_05y)
            ah.add_new_agegroups_10y(session=session, agegroups=agegroups_10y)
            ah.add_new_agegroups_rki(session=session, agegroups=agegroups_rki)


def main(db: Database):
    init_calendars(db, init.calendar_start_year, init.calendar_end_year)
    init_agegroups(db, init.agegroups_05y, init.agegroups_10y, init.agegroups_rki)


if __name__ == "__main__":
    db = Database(db_uri=f"{cfg_db.dialect}{cfg_db.name}.db")
    main(db=db)