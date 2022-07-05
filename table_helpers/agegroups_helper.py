from datetime import datetime
from typing import Any, Optional

import pandas as pd
from sqlalchemy.orm import Session

import database.create_database as tbl


def _get_min_age(agegroup: str) -> int:
    assert agegroup.__contains__("-"), "Agegroup must contain delimiter '-'"

    ages = agegroup.split("-")
    ages_int = [int(age) for age in ages]
    return min(ages_int)


def _get_max_age(agegroup: str) -> int:
    ages = agegroup.split("-")
    ages_int = [int(age) for age in ages]
    return max(ages_int)


def _nth_triangle_number(min_n: int, max_n) -> int:
    nth_triangle = 0
    for i in range(min_n, max_n + 1):
        nth_triangle += i
    return nth_triangle


def _calc_avg_age(agegroup: str, n_observations: int = 10) -> float:
    min_age = _get_min_age(agegroup=agegroup)
    max_age = _get_max_age(agegroup=agegroup)
    avg_age = _nth_triangle_number(min_n=min_age, max_n=max_age)
    return avg_age / n_observations

def get_agegroup_05y(session: Session, agegroup: str) -> Optional[Any]:
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
            session.query(tbl.Agegroups05y.agegroup).filter(
                tbl.Agegroups05y.agegroup == agegroup
            )
        ).one_or_none()
    return agegroup_05y_row


def get_agegroups_05y(self) -> list[tbl.Agegroups05y]:
    """
    Get a list of agegroup objects with a 05-year interval

    Args:
        session: `Session` object from `sqlalchemy.orm`

    Returns:
        A `list` of `Agegroups05y` objects
    """
    session = self.create_session()
    with session.begin():
        agegroups_05_list = (
            session.query(tbl.Agegroups05y).order_by(tbl.Agegroups05y.agegroup)
        ).all()
    return agegroups_05_list


def get_agegroup_10y(session: Session, agegroup: str) -> Optional[Any]:
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
            session.query(tbl.Agegroups10y.agegroup).filter(
                tbl.Agegroups10y.agegroup == agegroup
            )
        ).one_or_none()
    return agegroup_10y_row


def get_agegroups_10y(session: Session) -> list[tbl.Agegroups10y]:
    """
    Get a list of agegroup objects with a 10-year interval

    Args:
        session: `Session` object from `sqlalchemy.orm`

    Returns:
        A `list` of `Agegroups10y` objects
    """
    with session.begin():
        agegroups_10y_list = (
            session.query(tbl.Agegroups10y).order_by(tbl.Agegroups10y.agegroup)
        ).all()
    return agegroups_10y_list


def get_agegroup_rki(session: Session, agegroup: str) -> Optional[Any]:
    """
    Get a specific agegroup of RKI-year interval

    Args:
        session: `Session` object from `sqlalchemy.orm`
        agegroup: Specific agegroup of RKI-year interval

    Returns:
        A `row` object for the provided agegroup or `None` if calendar year not found.
    """
    with session.begin():
        agegroup_rki_row = (
            session.query(tbl.AgegroupsRki.agegroup).filter(
                tbl.AgegroupsRki.agegroup == agegroup
            )
        ).one_or_none()
    return agegroup_rki_row


def add_new_agegroups_05y(session: Session, agegroups: list[str]) -> None:
    """
    Adds a new agegroup with a 5-year-interval to the local SQLite database.

    Args:
        session: `Session` object from `sqlalchemy.orm`
        agegroup: Agegroup with a 5-year interval. Example: `"00-04"` or `"10-14"`
    """

    new_agegroups = list()

    for agegroup in agegroups:
        assert agegroup.__contains__("-"), "Agegroup must contain delimiter '-'"

        # check if agegroup already exist
        agegroup_exists = get_agegroup_05y(session=session, agegroup=agegroup)
        if agegroup_exists is not None:
            continue

        # create new agegroup
        new_agegroup = tbl.Agegroups05y(agegroup=agegroup, unique_key=agegroup)
        new_agegroups.append(new_agegroup)

    # write to DB
    with session.begin():
        session.add_all(new_agegroups)
        session.commit()


def add_new_agegroups_10y(session: Session, agegroups: list[str]) -> None:
    """
    Adds a new agegroup with a 10-year-interval to the local SQLite database.
    Calculates average age.
    Args:
        agegroup: Agegroup with a 10-year interval. Example: `"00-09"` or `"10-19"`
    """

    new_agegroups = list()

    for agegroup in agegroups:
        assert agegroup.__contains__("-"), "Agegroup must contain delimiter '-'"

        # check if agegroup already exist
        agegroup_exists = get_agegroup_10y(session=session, agegroup=agegroup)
        if agegroup_exists is not None:
            continue

        n_observations = (_get_max_age(agegroup) - _get_min_age(agegroup)) + 1
        # create new agegroup
        new_agegroup = tbl.Agegroups10y(
            agegroup=agegroup,
            number_observations=n_observations,
            avg_age=_calc_avg_age(agegroup, n_observations),
            unique_key=agegroup,
        )
        new_agegroups.append(new_agegroup)

    # add hardcoded special agegroups
    _EIGHTYPLUS = tbl.Agegroups10y(
        agegroup="80+", number_observations=21, avg_age=90.0, unique_key="80+"
    )
    _UNK = tbl.Agegroups10y(
        agegroup="UNK", number_observations=0, avg_age=0, unique_key="UNK"
    )

    if get_agegroup_10y(session=session, agegroup="80+") is None:
        new_agegroups.append(_EIGHTYPLUS)
    if get_agegroup_10y(session=session, agegroup="UNK") is None:
        new_agegroups.append(_UNK)

    # write to DB
    with session.begin():
        session.add_all(new_agegroups)
        session.commit()


def add_new_agegroups_rki(session: Session, agegroups: list[str]) -> None:
    """
    Adds a new agegroup with a year-interval defined by RKI (Robert-Koch_Institut).
    Calculates average age.
    Args:
        agegroup: Agegroup with the RKI-standard interval
    """

    new_agegroups = list()

    for agegroup in agegroups:
        assert agegroup.__contains__("-"), "Agegroup must contain delimiter '-'"
        
        # check if agegroup already exist
        agegroup_exists = get_agegroup_rki(session=session, agegroup=agegroup)
        if agegroup_exists is not None:
            continue

        n_observations = (_get_max_age(agegroup) - _get_min_age(agegroup)) + 1
        # create new agegroup
        new_agegroup = tbl.AgegroupsRki(
            agegroup=agegroup,
            number_observations=n_observations,
            avg_age=_calc_avg_age(agegroup, n_observations),
            unique_key=agegroup,
        )
        new_agegroups.append(new_agegroup)

    # add hardcoded special agegroups
    _EIGHTYPLUS = tbl.AgegroupsRki(
        agegroup="80+", number_observations=21, avg_age=90.0, unique_key="80+"
    )
    _UNK = tbl.AgegroupsRki(
        agegroup="UNK", number_observations=0, avg_age=0, unique_key="UNK"
    )

    if get_agegroup_rki(session=session, agegroup="80+") is None:
        new_agegroups.append(_EIGHTYPLUS)
    if get_agegroup_rki(session=session, agegroup="UNK") is None:
        new_agegroups.append(_UNK)

    # write to DB
    with session.begin():
        session.add_all(new_agegroups)
        session.commit()
