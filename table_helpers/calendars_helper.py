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


"""
def insert_calendar_years(session: Session, start_year: int, end_year: int) -> None:

    #Inserts Calendar Years, ISO-weeks and ISO-days to the local SQLite database.

    #Args:
    #    years: A list of years as `int`

    #assert start_year < end_year, "`start_year` can not be greater than `end_year`!"
    #assert start_year >= 0, "`start_year can not be negative!"
    #assert end_year >= 0, "`end_year can not be negative!"

    YEAR_RANGE = [year for year in range(start_year, end_year + 1)]

    def _add_calendar_weeks_n_days_to_session(year_obj: tbl.CalendarYear) -> None:
     
        #Helper function to create and add iso weeks and days to the session.
     
        # create a date-range to figure out iso weeks and years
        date_range = pd.date_range(
            date(year_obj.iso_year, 1, 1), date(year_obj.iso_year, 12, 31)  # type: ignore
        )

        # loop the date-range
        for single_date in date_range:
            # avoid inserting iso years that don't exist in DB
            calendar_year = get_calendar_year(
                session=session, iso_year=single_date.isocalendar().year
            )
            if calendar_year is None:
                continue

            # create and check iso_key (year+week)
            # iso_key is a unique constraint, so continue if exist
            iso_key_str = str(single_date.isocalendar().year) + str(
                single_date.isocalendar().week
            ).zfill(2)
            iso_key = int(iso_key_str)  # cast back to int
            if iso_key_exist(session, iso_key):
                continue

            # create new week obj
            new_calendar_week = tbl.CalendarWeek(
                calendar_year_fk=year_obj.calendar_year_id,
                iso_week=single_date.isocalendar().week,
                iso_key=iso_key,
            )
            session.add(new_calendar_week)
            session.flush()

            # now add days of this week for this year
            new_calendar_day = tbl.CalendarDay(
                calendar_week_fk=new_calendar_week.calendar_week_id,
                iso_day=single_date.date(),
            )
            session.add(new_calendar_day)
            session.flush()

    # loop all the provided years
    for iso_year in YEAR_RANGE:
        # check if calendar year already exist
        year_obj = get_calendar_year(session=session, iso_year=iso_year)
        if year_obj is not None:  # year exists
            # check if calendar weeks already exist
            # function returns a list or Rows, so check if list is empty
            weeks_exists = get_calendar_weeks(session=session, iso_year=iso_year)
            if weeks_exists:
                continue
            else:
                # create weeks
                _add_calendar_weeks_n_days_to_session(year_obj=year_obj)
                continue

        # add years
        new_calendar_year = tbl.CalendarYear(iso_year=iso_year)
        session.add(new_calendar_year)
        session.flush()

        # create weeks
        _add_calendar_weeks_n_days_to_session(year_obj=new_calendar_year)

    session.commit()
"""
