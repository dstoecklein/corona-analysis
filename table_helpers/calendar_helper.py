from datetime import date, datetime
from typing import Any, Optional

import pandas as pd
from sqlalchemy.orm import Session

import database.create_database as tbl


# TODO: Refactor create_df functions to use SQLalchemy
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

    df_years["unique_key"] = df_years["iso_year"]
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

    df_weeks["unique_key"] = df_weeks["iso_key"]
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

    df_day["unique_key"] = df_day["iso_day"]
    df_day["created_on"] = datetime.now()
    df_day["updated_on"] = datetime.now()

    return df_day


def get_calendar_year(session: Session, year: int) -> Optional[Any]:
    """
    Get a specific calendar year

    Args:
        session: `Session` object from `sqlalchemy.orm`
        calendar_year: Specific calendar year

    Returns:
        A `row` object for the provided calendar year or `None`
        if calendar year not found.
    """
    with session.begin():
        calendar_year_row = (
            session.query(tbl.CalendarYears.iso_year).filter(
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
    with session.begin():
        calendar_years_list = (
            session.query(tbl.CalendarYears).order_by(tbl.CalendarYears.iso_year)
        ).all()
    return calendar_years_list


def get_calendar_weeks(session: Session) -> list[tbl.CalendarWeeks]:
    """
    Get all calendar weeks objects as list

    Args:
        session: `Session` object from `sqlalchemy.orm`

    Returns:
        A `list` of `CalendarWeeks` objects.
    """
    with session.begin():
        calendar_weeks_list = (
            session.query(tbl.CalendarWeeks).order_by(tbl.CalendarWeeks.iso_key)
        ).all()
    return calendar_weeks_list


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
        new_calendar_year = tbl.CalendarYears(iso_year=year, unique_key=str(year))
        new_years.append(new_calendar_year)

    # write to DB
    with session.begin():
        session.add_all(new_years)
        session.commit()
