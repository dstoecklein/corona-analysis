from datetime import date, datetime
from typing import Any, Optional

import pandas as pd
from sqlalchemy.engine.row import Row
from sqlalchemy.orm import Session

import database.tables as tbl


def _create_base_calendar_df(start_year: int, end_year: int) -> pd.DataFrame:
    start_date = date(start_year, 1, 1)
    end_date = date(end_year, 12, 31)
    date_range = pd.date_range(start_date, end_date)

    calendar_dict = dict()

    for single_date in date_range:
        iso_values_list = list()
        regular_day = single_date.strftime("%Y-%m-%d")
        iso_week = single_date.isocalendar().week
        iso_year = single_date.isocalendar().year

        # fill list with iso values
        iso_values_list.append(iso_week)
        iso_values_list.append(iso_year)

        # fill calendar dict {regular_day: iso_values}
        calendar_dict[regular_day] = iso_values_list

    # create dataframe
    df_base = pd.DataFrame.from_dict(calendar_dict, orient="index").reset_index(level=0)
    df_base.columns = ["iso_day", "iso_week", "iso_year"]
    df_base["iso_week"] = df_base["iso_week"].astype(str).str.zfill(2)
    df_base["iso_key"] = df_base["iso_year"].astype(str) + df_base["iso_week"].astype(
        str
    )
    df_base.index += 1  # start index at 1

    return df_base


def create_calendar_years_df(start_year: int, end_year: int) -> pd.DataFrame:
    df_base = _create_base_calendar_df(start_year, end_year)
    df_years = (
        df_base.groupby(["iso_year"], as_index=False)
        .agg({"iso_day": "first", "iso_week": "first", "iso_key": "first"})
        .copy()
    )
    df_years.index = df_years.index.set_names(["ID"])
    df_years.index += 1
    df_years = df_years.reset_index().rename(
        columns={df_years.index.name: "calendar_years_id"}
    )
    df_years = df_years.iloc[:, :2]  # only first two cols

    df_years["created_on"] = datetime.now()
    df_years["updated_on"] = datetime.now()

    return df_years


def create_calendar_weeks_df(start_year: int, end_year: int) -> pd.DataFrame:
    df_base = _create_base_calendar_df(start_year, end_year)
    df_years = create_calendar_years_df(start_year, end_year)

    df_weeks = (
        df_base.groupby(["iso_key"], as_index=False)
        .agg({"iso_day": "first", "iso_week": "first", "iso_year": "first"})
        .copy()
    )

    # create foreign key column to calendar_years_id
    df_weeks = df_weeks.merge(
        df_years, how="left", left_on="iso_year", right_on="iso_year"
    )
    df_weeks.index = df_weeks.index.set_names(["ID"])
    df_weeks.index += 1
    df_weeks = df_weeks.reset_index().rename(
        columns={df_weeks.index.name: "calendar_weeks_id"}
    )
    df_weeks.rename(columns={"calendar_years_id": "calendar_years_fk"}, inplace=True)
    df_weeks["iso_week"] = df_weeks["iso_week"].astype(int)
    df_weeks.drop(["iso_day", "iso_year"], axis=1, inplace=True)

    df_weeks["created_on"] = datetime.now()
    df_weeks["updated_on"] = datetime.now()

    return df_weeks


def create_calendar_days_df(start_year: int, end_year: int) -> pd.DataFrame:
    df_base = _create_base_calendar_df(start_year, end_year)
    df_weeks = create_calendar_weeks_df(start_year, end_year)

    # create foreign key column to calendar_weeks_id
    df_day = df_base.merge(
        df_weeks, how="left", left_on="iso_key", right_on="iso_key", suffixes=("", "_y")
    )
    df_day.drop(df_day.filter(regex="_y$").columns, axis=1, inplace=True)
    df_day.drop(
        ["iso_year", "iso_week", "iso_key", "calendar_years_fk"], axis=1, inplace=True
    )
    df_day.rename(columns={"calendar_weeks_id": "calendar_weeks_fk"}, inplace=True)

    df_day["created_on"] = datetime.now()
    df_day["updated_on"] = datetime.now()

    return df_day


def get_calendar_year(session: Session, year: int) -> Optional[tbl.CalendarYears]:
    """
    Get a specific calendar year

    Args:
        session: `Session` object from `sqlalchemy.orm`
        calendar_year: Specific calendar year

    Returns:
        A `row` object for the provided calendar year or `None`
        if calendar year not found.
    """
    calendar_year_row = (
        session.query(tbl.CalendarYears).filter(
            tbl.CalendarYears.iso_year == year
        )
    ).one_or_none()
    return calendar_year_row


def get_calendar_years(session: Session) -> list[tbl.CalendarYears]:
    """
    Get all calendar years objects as list

    Args:
        session: `Session` object from `sqlalchemy.orm`

    Returns:
        A `list` of `CalendarYears` objects.
    """
    calendar_years_list = (
        session.query(tbl.CalendarYears).order_by(tbl.CalendarYears.iso_year)
    ).all()
    return calendar_years_list


def get_calendar_weeks(session: Session, iso_year: int) -> list[Row]:
    """
    Get all calendar weeks for a given year (ISO)

    Args:
        session: `Session` object from `sqlalchemy.orm`
        iso_year: Year

    Returns:
        A `list` of `Row`'s.
    """

    # query
    row = (
        session.query(tbl.CalendarWeeks.iso_week, tbl.CalendarWeeks.iso_key)
        .join(tbl.CalendarYears)
        .filter(tbl.CalendarYears.iso_year == iso_year)
    ).all()
    return row


def iso_key_exist(session: Session, iso_key: int):
    # query
    row = (
        session.query(
            tbl.CalendarWeeks.iso_key
        )
        .filter(
            tbl.CalendarWeeks.iso_key == iso_key
        )
    ).one_or_none()

    if row is None:
        return False
    return True


def get_calendar_days(session: Session, iso_year: int) -> list[Row]:
    """
    Get all calendar days for a given year (ISO)

    Args:
        session: `Session` object from `sqlalchemy.orm`
        iso_year: Year

    Returns:
        A `list` of `Row`'s.
    """

    # query
    row = (
        session.query(
            tbl.CalendarDays.iso_day,
            tbl.CalendarWeeks.iso_week,
            tbl.CalendarWeeks.iso_key,
        )
        .join(
            tbl.CalendarWeeks,
            tbl.CalendarDays.calendar_weeks_fk == tbl.CalendarWeeks.calendar_weeks_id,
        )
        .join(
            tbl.CalendarYears,
            tbl.CalendarWeeks.calendar_years_fk == tbl.CalendarYears.calendar_years_id,
        )
        .filter(tbl.CalendarYears.iso_year == iso_year)
    ).all()
    return row


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
        new_calendar_year = tbl.CalendarYears(iso_year=year)
        new_years.append(new_calendar_year)

    # write to DB
    session.add_all(new_years)
    session.commit()

# TODO: When new year is added, autofill weeks and days
def add_new_calendar_years2(session: Session, years: list[int]) -> None:
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
        new_calendar_year = tbl.CalendarYears(iso_year=year)

        new_years.append(new_calendar_year)

    # write years to DB
    session.add_all(new_years)
    session.commit()
        
    for n in new_years:
        daterange = pd.date_range(date(n.iso_year, 1, 1), date(n.iso_year, 12, 31))
        new_calendar_weeks = list()

        for single_date in daterange:
            calendar_year = get_calendar_year(session, single_date.isocalendar().year)
            if calendar_year is None:
                continue

            # create and check iso_key
            iso_key = str(single_date.isocalendar().year) + str(single_date.isocalendar().week).zfill(2)
            if iso_key_exist(session, int(iso_key)):
                continue

            # new entry
            new_calendar_week = tbl.CalendarWeeks(
                calendar_years_fk=calendar_year.calendar_years_id,
                iso_week=single_date.isocalendar().week,
                iso_key=iso_key,
            )

            new_calendar_weeks.append(new_calendar_week)
            
            # write weeks to DB
            session.add_all(new_calendar_weeks)
            session.commit()

    # TODO: Auto-add calendar days