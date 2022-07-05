import table_helpers.agegroups_helper as ah
import table_helpers.calendar_helper as ch
from config.core import cfg_table_names as tbl
from database.db_helper import Database

DB = Database()


def init_calendars(start_year: int, end_year: int) -> None:
    df_years = ch.create_calendar_years_df(start_year, end_year)
    df_weeks = ch.create_calendar_weeks_df(start_year, end_year)
    df_days = ch.create_calendar_days_df(start_year, end_year)

    DB.upsert_df(df=df_years, table_name=tbl.calendar_years)
    DB.upsert_df(df=df_weeks, table_name=tbl.calendar_weeks)
    DB.upsert_df(df=df_days, table_name=tbl.calendar_days)


def init_agegroups(agegroups_05y: list, agegroups_10y: list, agegroups_rki: list) -> None:
    session = DB.create_session()

    ah.add_new_agegroups_05y(session=session, agegroups=agegroups_05y)
    ah.add_new_agegroups_10y(session=session, agegroups=agegroups_10y)
    ah.add_new_agegroups_rki(session=session, agegroups=agegroups_rki)

if __name__ == "__main__":
    # init_calendars(1990, 2050)
    
    agegroups_05y = [
        "00-04",
        "05-09",
        "10-14",
        "15-19",
        "20-24",
        "25-29",
        "30-34",
        "35-39",
        "40-44",
        "45-49",
        "50-54",
        "55-59",
        "60-64",
        "65-69",
        "70-74",
        "75-79"
    ]

    agegroups_10y = [
        "00-09",
        "10-19",
        "20-29",
        "30-39",
        "40-49",
        "50-59",
        "60-69",
        "70-79"
    ]

    agegroups_rki = [
        "00-04",
        "05-14",
        "15-34",
        "35-59",
        "60-79"
    ]

    init_agegroups(agegroups_05y, agegroups_10y, agegroups_rki)
