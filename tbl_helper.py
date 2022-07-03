import uuid
import pandas as pd
from sqlalchemy import Table
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.engine.row import Row
from sqlalchemy.orm import Session

from db_helper2 import Database
import create_tables as tbl


DB = Database()


def _get_min_age(agegroup: str) -> int:
    assert agegroup.__contains__("-"), "Agegroup must contain delimiter '-'"

    ages = agegroup.split("-")
    ages = [int(age) for age in ages]
    return min(ages) 


def _get_max_age(agegroup: str) -> int:
    assert agegroup.__contains__("-"), "Agegroup must contain delimiter '-'"

    ages = agegroup.split("-")
    ages = [int(age) for age in ages]
    return max(ages) 


def _nth_triangle_number(min_n: int, max_n) -> int:
    nth_triangle = 0
    for i in range(min_n, max_n+1):
        nth_triangle += i
    return nth_triangle


def _calc_avg_age(agegroup: str, n_observations: int = 10) -> float:
    min_age = _get_min_age(agegroup=agegroup)
    max_age = _get_max_age(agegroup=agegroup)
    avg_age = _nth_triangle_number(min_n=min_age, max_n=max_age)
    return avg_age / n_observations


def check_if_table_exists(table_name: str):
    try:
        Table(table_name, DB.metadata, autoload=True)
        return True
    except NoSuchTableError:
        return False


def get_table_name(table_name: str):
    try:
        return Table(table_name, DB.metadata, autoload=True)
    except NoSuchTableError:
        return None


def upsert_df(session: Session, df: pd.DataFrame, table_name: str) -> None:
    """
    UPSERT (UPDATE if exist, INSERT if not exist) rows of a `pandas.DataFrame``to the local SQLite database.
    Credit: https://stackoverflow.com/questions/61366664/how-to-upsert-pandas-dataframe-to-postgresql-table
    
    Args:
        session: `Session` object from `sqlalchemy.orm`
        df: `pandas.DataFrame` to be updated/inserted
        table_name: Name of table where `df` should be updated/inserted
    """

    # df must contain a unique_key column
    if "unique_key" not in df.columns:
        raise RuntimeError("DataFrame must contain a 'unique_key' column")

    # check if table exist. If not, create it using to_sql
    table_exist = check_if_table_exists(table_name=table_name)
    if not table_exist:
        df.to_sql(table_name, DB.engine)
        return     

    # table exist, so use UPSERT logic...
    
    # 1. create temporary table with unique id
    tmp_table = f"tmp_{uuid.uuid4().hex[:6]}"
    df.to_sql(tmp_table, DB.engine, index=True)

    # 2. create column name strings
    columns = list(df.columns)
    columns_str = ", ".join(col for col in columns)

    # The "excluded." prefix causes the column to refer to the value that 
    # would have been inserted if there been no conflict.
    update_columns_str = ", ".join(
        f'{col} = excluded.{col}' for col in columns
    )

    # 3. create sql query
    query_upsert = f"""
        INSERT INTO {table_name}({columns_str})
        SELECT {columns_str} FROM {tmp_table} WHERE true
        ON CONFLICT(unique_key) DO UPDATE SET
        {update_columns_str};
    """

    # 4. execute upsert query & drop temporary table
    session.execute(query_upsert)
    session.execute(f"DROP TABLE {tmp_table}")


def get_agegroup_05y(session: Session, agegroup: str) -> Row:
    """
    Get a specific agegroup of 05-year interval

    Args:
        session: `Session` object from `sqlalchemy.orm`
        agegroup: Specific agegroup of 05-year interval

    Returns:
        A `row` object for the provided agegroup or `None` if calendar year not found.
    """
    return (
        session.query(tbl.Agegroups_05y.agegroup)
        .filter(tbl.Agegroups_05y.agegroup == agegroup)
    ).one_or_none()


def get_agegroups_05y(session: Session) -> list[tbl.Agegroups_05y]:
    """
    Get a list of agegroup objects with a 05-year interval

    Args:
        session: `Session` object from `sqlalchemy.orm`

    Returns:
        A `list` of `Agegroups_05y` objects
    """
    return (
        session.query(tbl.Agegroups_05y)
        .order_by(tbl.Agegroups_05y.agegroup)
    ).all()


def get_agegroup_10y(session: Session, agegroup: str) -> Row:
    """
    Get a specific agegroup of 10-year interval

    Args:
        session: `Session` object from `sqlalchemy.orm`
        agegroup: Specific agegroup of 10-year interval

    Returns:
        A `row` object for the provided agegroup or `None` if calendar year not found.
    """
    return (
        session.query(tbl.Agegroups_10y.agegroup)
        .filter(tbl.Agegroups_10y.agegroup == agegroup)
    ).one_or_none()


def get_agegroups_10y(session: Session) -> list[tbl.Agegroups_10y]:
    """
    Get a list of agegroup objects with a 10-year interval

    Args:
        session: `Session` object from `sqlalchemy.orm`

    Returns:
        A `list` of `Agegroups_10y` objects
    """
    return (
        session.query(tbl.Agegroups_10y)
        .order_by(tbl.Agegroups_10y.agegroup)
    ).all()


def get_calendar_year(session: Session, year: int) -> Row:
    """
    Get a specific calendar year

    Args:
        session: `Session` object from `sqlalchemy.orm`
        calendar_year: Specific calendar year

    Returns:
        A `row` object for the provided calendar year or `None` if calendar year not found.
    """
    return (
        session.query(tbl.CalendarYears.iso_year)
        .filter(tbl.CalendarYears.iso_year == year)
    ).one_or_none()


def get_calendar_years(session: Session) -> list[tbl.CalendarYears]:
    """
    Get all calendar year objects as list

    Args:
        session: `Session` object from `sqlalchemy.orm`

    Returns:
        A `list` of `CalendarYears` objects.
    """
    return (
        session.query(tbl.CalendarYears)
        .order_by(tbl.CalendarYears.iso_year)
    ).all()


def add_new_agegroups_05y(session: Session, agegroups: list[str]) -> None:
    """
    Adds a new agegroup with a 5-year-interval to the local SQLite database.

    Args:
        session: `Session` object from `sqlalchemy.orm`
        agegroup: Agegroup with a 5-year interval. Example: `"00-04"` or `"10-14"`
    """

    new_agegroups = list()

    for agegroup in agegroups:
        # check if agegroup already exist
        agegroup_exists = get_agegroup_05y(session=session, agegroup=agegroup)
        if agegroup_exists is not None:
            continue
        
        # create new agegroup
        new_agegroup = tbl.Agegroups_05y(
            agegroup=agegroup,
            unique_key=agegroup
        )
        new_agegroups.append(new_agegroup)

    # write to DB
    session.add_all(new_agegroups)
    session.commit()


def add_new_agegroup_10y(session: Session, agegroups: list[str]) -> None:
    """
    Adds a new agegroup with a 10-year-interval to the local SQLite database.
    Calculates average age.
    Args:
        agegroup: Agegroup with a 10-year interval. Example: `"00-09"` or `"10-19"`
    """
    
    new_agegroups = list()

    for agegroup in agegroups:
        # check if agegroup already exist
        agegroup_exists = get_agegroup_10y(session=session, agegroup=agegroup)
        if agegroup_exists is not None:
            continue
    
        # create new agegroup
        new_agegroup = tbl.Agegroups_10y(
            agegroup=agegroup, 
            number_observations=10, 
            avg_age=_calc_avg_age(agegroup, n_observations=10),
            unique_key=agegroup
        )
        new_agegroups.append(new_agegroup)

    # add hardcoded special agegroups
    EIGHTYPLUS = new_agegroup = tbl.Agegroups_10y(
        agegroup="80+",
        number_observations=21,
        avg_age=90.0,
        unique_key="80+"
    )
    UNK = new_agegroup = tbl.Agegroups_10y(
        agegroup="UNK",
        number_observations=0,
        avg_age=0,
        unique_key="UNK"
    )
    
    if get_agegroup_10y(session=session, agegroup="80+") is None:
        new_agegroups.append(EIGHTYPLUS)
    if get_agegroup_10y(session=session, agegroup="UNK") is None:
        new_agegroups.append(UNK)

    # write to DB
    session.add_all(new_agegroups)
    session.commit()


def add_new_calendar_years(session: Session, years: list[int]) -> None:
    """
    Adds a list of new calendar years to the local SQLite database.

    Args:
        calendar_year: A list of calendar year
    """
    
    new_years = list()

    for year in years:
        # check if calendar year already exist
        year_exists = get_calendar_year(session=session, year=year)
        if year_exists is not None:
            continue

        # create new entry
        new_calendar_year = tbl.CalendarYears(
            iso_year=year,
            unique_key=str(year)
        )
        new_years.append(new_calendar_year)
    
    # write to DB
    session.add_all(new_years)
    session.commit()


if __name__ == "__main__":
    session = DB.create_session()
    with session:
        with session.begin():
            #new_years = [year for year in range(1990, 2051)]
            #add_new_calendar_years(session=session, years=new_years)
            """
            new_agegroups_05y = [
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
                "75-79",
                "80+",
                "UNK"
            ]
            add_new_agegroups_05y(session=session, agegroups=new_agegroups_05y)
            
            new_agegroups_10y = [
                "00-09",
                "10-19",
                "20-29",
                "30-39",
                "40-49",
                "50-59",
                "60-69",
                "70-79"
            ]
            add_new_agegroup_10y(session=session, agegroups=new_agegroups_10y)
            """

            import isoweek
            import numpy as np
            from datetime import datetime as dt

            # Map Years with its Foreign Keys
            calendar_years = get_calendar_years(session=session)
            calendar_years_dict = {}
            calendar_years_dict_reverse = {}
            for year in calendar_years:
                calendar_years_dict[year.iso_year] = year.calendar_years_id
            for year in calendar_years:
                calendar_years_dict_reverse[year.calendar_years_id] = year.iso_year

            calendar_weeks_dict = {}

            for year, calendar_years_fk in calendar_years_dict.items():
                weeks_list = []
                weeks = isoweek.Week.last_week_of_year(year).week
                for week in range(1, weeks+1):
                    weeks_list.append(week)
                calendar_weeks_dict[calendar_years_fk] = weeks_list

            df = pd.DataFrame.from_dict(calendar_weeks_dict, orient="index")
            df = df.T.unstack().to_frame().dropna().reset_index(level=0).applymap(np.int64)
            df.columns = ["calendar_years_fk", "iso_week"]
            df["iso_key"] = df["calendar_years_fk"].map(calendar_years_dict_reverse).astype(str) + df["iso_week"].astype(str).str.zfill(2)
            df["unique_key"] = df["iso_key"]
            df["created_on"] = dt.now()
            df["updated_on"] = dt.now()

            upsert_df(session, df, "_calendar_weeks")
