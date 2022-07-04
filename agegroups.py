import uuid
import pandas as pd
from sqlalchemy import Table
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.engine.row import Row
from sqlalchemy.orm import Session

import create_tables as tbl


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


def get_agegroup_05y(session: Session, agegroup: str) -> Row:
    """
    Get a specific agegroup of 05-year interval

    Args:
        session: `Session` object from `sqlalchemy.orm`
        agegroup: Specific agegroup of 05-year interval

    Returns:
        A `row` object for the provided agegroup or `None` if calendar year not found.
    """

    with session.begin():
        agegroup_05y_row = (
            session.query(tbl.Agegroups_05y.agegroup)
            .filter(tbl.Agegroups_05y.agegroup == agegroup)
        ).one_or_none()
    return agegroup_05y_row


def get_agegroups_05y(self) -> list[tbl.Agegroups_05y]:
    """
    Get a list of agegroup objects with a 05-year interval

    Args:
        session: `Session` object from `sqlalchemy.orm`

    Returns:
        A `list` of `Agegroups_05y` objects
    """
    session = self.create_session()
    with session.begin():
        agegroups_05_list = (
            session.query(tbl.Agegroups_05y)
            .order_by(tbl.Agegroups_05y.agegroup)
        ).all()
    return agegroups_05_list


def get_agegroup_10y(session: Session, agegroup: str) -> Row:
    """
    Get a specific agegroup of 10-year interval

    Args:
        session: `Session` object from `sqlalchemy.orm`
        agegroup: Specific agegroup of 10-year interval

    Returns:
        A `row` object for the provided agegroup or `None` if calendar year not found.
    """
    with session.begin():
        agegroup_10y_row = (
            session.query(tbl.Agegroups_10y.agegroup)
            .filter(tbl.Agegroups_10y.agegroup == agegroup)
        ).one_or_none()
    return agegroup_10y_row


def get_agegroups_10y(session: Session) -> list[tbl.Agegroups_10y]:
    """
    Get a list of agegroup objects with a 10-year interval

    Args:
        session: `Session` object from `sqlalchemy.orm`

    Returns:
        A `list` of `Agegroups_10y` objects
    """
    with session.begin():
        agegroups_10y_list = (
            session.query(tbl.Agegroups_10y)
            .order_by(tbl.Agegroups_10y.agegroup)
        ).all()
    return agegroups_10y_list


def get_calendar_year(session: Session, year: int) -> Row:
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
            session.query(tbl.CalendarYears.iso_year)
            .filter(tbl.CalendarYears.iso_year == year)
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
            session.query(tbl.CalendarYears)
            .order_by(tbl.CalendarYears.iso_year)
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
            session.query(tbl.CalendarWeeks)
            .order_by(tbl.CalendarWeeks.iso_key)
        ).all()
    return calendar_weeks_list


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
    with session.begin():
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
    _EIGHTYPLUS = new_agegroup = tbl.Agegroups_10y(
        agegroup="80+",
        number_observations=21,
        avg_age=90.0,
        unique_key="80+"
    )
    _UNK = new_agegroup = tbl.Agegroups_10y(
        agegroup="UNK",
        number_observations=0,
        avg_age=0,
        unique_key="UNK"
    )
    
    if get_agegroup_10y(session=session, agegroup="80+") is None:
        new_agegroups.append(_EIGHTYPLUS)
    if get_agegroup_10y(session=session, agegroup="UNK") is None:
        new_agegroups.append(_UNK)

    # write to DB
    with session.begin():
        session.add_all(new_agegroups)
        session.commit()