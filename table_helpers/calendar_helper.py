from typing import Any, Optional

from sqlalchemy.orm import Session

import database.create_tables as tbl


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
