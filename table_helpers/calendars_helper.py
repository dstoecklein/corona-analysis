from datetime import date, datetime
from typing import Optional

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


def create_calendar_year_df(start_year: int, end_year: int) -> pd.DataFrame:
    df_base = _create_base_calendar_df(start_year, end_year)
    df_years = (
        df_base.groupby(["iso_year"], as_index=False)
        .agg({"iso_day": "first", "iso_week": "first", "iso_key": "first"})
        .copy()
    )
    df_years.index = df_years.index.set_names(["ID"])
    df_years.index += 1
    df_years = df_years.reset_index().rename(
        columns={df_years.index.name: "calendar_year_id"}
    )
    df_years = df_years.iloc[:, :2]  # only first two cols

    df_years["created_on"] = datetime.now()
    df_years["updated_on"] = datetime.now()

    return df_years


def create_calendar_week_df(start_year: int, end_year: int) -> pd.DataFrame:
    df_base = _create_base_calendar_df(start_year, end_year)
    df_years = create_calendar_year_df(start_year, end_year)

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
        columns={df_weeks.index.name: "calendar_week_id"}
    )
    df_weeks.rename(columns={"calendar_year_id": "calendar_year_fk"}, inplace=True)
    df_weeks["iso_week"] = df_weeks["iso_week"].astype(int)
    df_weeks.drop(["iso_day", "iso_year"], axis=1, inplace=True)

    df_weeks["created_on"] = datetime.now()
    df_weeks["updated_on"] = datetime.now()

    return df_weeks


def create_calendar_day_df(start_year: int, end_year: int) -> pd.DataFrame:
    df_base = _create_base_calendar_df(start_year, end_year)
    df_weeks = create_calendar_week_df(start_year, end_year)

    # create foreign key column to calendar_weeks_id
    df_day = df_base.merge(
        df_weeks, how="left", left_on="iso_key", right_on="iso_key", suffixes=("", "_y")
    )
    df_day.drop(df_day.filter(regex="_y$").columns, axis=1, inplace=True)
    df_day.drop(
        ["iso_year", "iso_week", "iso_key", "calendar_year_fk"], axis=1, inplace=True
    )
    df_day.rename(columns={"calendar_week_id": "calendar_week_fk"}, inplace=True)

    df_day["created_on"] = datetime.now()
    df_day["updated_on"] = datetime.now()

    return df_day


def get_calendar_year(session: Session, iso_year: int) -> Optional[tbl.CalendarYear]:
    """
    Get a specific calendar year

    Args:
        session: `Session` object from `sqlalchemy.orm`
        calendar_year: Specific calendar year

    Returns:
        A `row` object for the provided calendar year or `None`
        if calendar year not found.
    """
    row = (
        session.query(tbl.CalendarYear).filter(tbl.CalendarYear.iso_year == iso_year)
    ).one_or_none()
    return row


def get_calendar_years(session: Session) -> list[tbl.CalendarYear]:
    """
    Get all calendar years objects as list

    Args:
        session: `Session` object from `sqlalchemy.orm`

    Returns:
        A `list` of `CalendarYear` objects.
    """
    calendar_years = (
        session.query(tbl.CalendarYear).order_by(tbl.CalendarYear.iso_year)
    ).all()
    return calendar_years


def get_calendar_years_df(session: Session) -> pd.DataFrame:
    """
    Get all calendar years objects as list

    Args:
        session: `Session` object from `sqlalchemy.orm`

    Returns:
        Pandas DataFrame
    """
    years_df = pd.read_sql(
        session.query(
            tbl.CalendarYear.calendar_year_id,
            tbl.CalendarYear.iso_year,
        )
        .order_by(tbl.CalendarYear.iso_year)
        .statement,
        session.bind,
    )
    return years_df


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
    rows = (
        session.query(tbl.CalendarWeek.iso_week, tbl.CalendarWeek.iso_key)
        .join(tbl.CalendarYear)
        .filter(tbl.CalendarYear.iso_year == iso_year)
    ).all()
    return rows


def get_calendar_weeks_df(session: Session) -> pd.DataFrame:
    """
    Get all calendar weeks objects as pandas dataframe

    Args:
        session: `Session` object from `sqlalchemy.orm`

    Returns:
        Pandas DataFrame
    """
    weeks_df = pd.read_sql(
        session.query(
            tbl.CalendarWeek.calendar_week_id,
            tbl.CalendarWeek.calendar_year_fk,
            tbl.CalendarWeek.iso_week,
            tbl.CalendarWeek.iso_key,
        )
        .order_by(tbl.CalendarWeek.iso_week)
        .statement,
        session.bind,
    )
    return weeks_df


def iso_key_exist(session: Session, iso_key: int) -> bool:
    # query
    row = (
        session.query(tbl.CalendarWeek.iso_key).filter(
            tbl.CalendarWeek.iso_key == iso_key
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
    rows = (
        session.query(
            tbl.CalendarDay.iso_day,
            tbl.CalendarWeek.iso_week,
            tbl.CalendarWeek.iso_key,
        )
        .join(
            tbl.CalendarWeek,
            tbl.CalendarDay.calendar_week_fk == tbl.CalendarWeek.calendar_week_id,
        )
        .join(
            tbl.CalendarYear,
            tbl.CalendarWeek.calendar_year_fk == tbl.CalendarYear.calendar_year_id,
        )
        .filter(tbl.CalendarYear.iso_year == iso_year)
    ).all()
    return rows
