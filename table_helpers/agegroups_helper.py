from typing import Any, Optional
import pandas as pd
from sqlalchemy.orm import Session

import database.tables as tbl


def _get_min_age(agegroup: str) -> int:
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


def get_agegroup05y(session: Session, agegroup: str) -> Optional[Any]:
    """
    Get a specific agegroup of 05-year interval

    Args:
        session: `Session` object from `sqlalchemy.orm`
        agegroup: Specific agegroup of 05-year interval

    Returns:
        A `row` object for the provided agegroup or `None` if calendar year not found.
    """
    row = (
        session.query(tbl.Agegroup05y.agegroup).filter(
            tbl.Agegroup05y.agegroup == agegroup
        )
    ).one_or_none()
    return row


def get_agegroups05y(session: Session) -> list[tbl.Agegroup05y]:
    """
    Get a list of agegroup objects with a 05-year interval

    Args:
        session: `Session` object from `sqlalchemy.orm`

    Returns:
        A `list` of `Agegroup05y` objects
    """
    agegroups05 = (
        session.query(tbl.Agegroup05y).order_by(tbl.Agegroup05y.agegroup)
    ).all()
    return agegroups05


def get_agegroup10y(session: Session, agegroup: str) -> Optional[Any]:
    """
    Get a specific agegroup of 10-year interval

    Args:
        session: `Session` object from `sqlalchemy.orm`
        agegroup: Specific agegroup of 10-year interval

    Returns:
        A `row` object for the provided agegroup or `None` if calendar year not found.
    """
    row = (
        session.query(tbl.Agegroup10y.agegroup).filter(
            tbl.Agegroup10y.agegroup == agegroup
        )
    ).one_or_none()
    return row


def get_agegroups10y(session: Session) -> list[tbl.Agegroup10y]:
    """
    Get a list of agegroup objects with a 10-year interval

    Args:
        session: `Session` object from `sqlalchemy.orm`

    Returns:
        A `list` of `Agegroup10y` objects
    """
    agegroups10y = (
        session.query(tbl.Agegroup10y).order_by(tbl.Agegroup10y.agegroup)
    ).all()
    return agegroups10y

def get_agegroups10y_df(session: Session) -> pd.DataFrame:
    """
    Get all agegroups of 10y-interval as pandas dataframe

    Args:
        session: `Session` object from `sqlalchemy.orm`

    Returns:
        Pandas DataFrame
    """
    agegroups10y = pd.read_sql(
        session.query(
            tbl.Agegroup10y.agegroup_10y_id,
            tbl.Agegroup10y.agegroup,
            tbl.Agegroup10y.number_observations,
            tbl.Agegroup10y.avg_age,
        )
        .order_by(tbl.Agegroup10y.agegroup)
        .statement,
        session.bind
    )
    return agegroups10y

def get_agegroup_rki(session: Session, agegroup: str) -> Optional[Any]:
    """
    Get a specific agegroup of RKI-year interval

    Args:
        session: `Session` object from `sqlalchemy.orm`
        agegroup: Specific agegroup of RKI-year interval

    Returns:
        A `row` object for the provided agegroup or `None` if calendar year not found.
    """
    row = (
        session.query(tbl.AgegroupRki.agegroup).filter(
            tbl.AgegroupRki.agegroup == agegroup
        )
    ).one_or_none()
    return row

def get_agegroups_rki_df(session: Session) -> pd.DataFrame:
    """
    Get all agegroups of RKI-interval as pandas dataframe

    Args:
        session: `Session` object from `sqlalchemy.orm`

    Returns:
        Pandas DataFrame
    """
    agegroups_rki = pd.read_sql(
        session.query(
            tbl.AgegroupRki.agegroup_rki_id,
            tbl.AgegroupRki.agegroup,
            tbl.AgegroupRki.number_observations,
            tbl.AgegroupRki.avg_age,
        )
        .order_by(tbl.AgegroupRki.agegroup)
        .statement,
        session.bind
    )
    return agegroups_rki

def insert_agegroups05y(session: Session, agegroups: list[str]) -> None:
    """
    Adds a new agegroup with a 5-year-interval to the local SQLite database.

    Args:
        session: `Session` object from `sqlalchemy.orm`
        agegroup: Agegroup with a 5-year interval. Example: `"00-04"` or `"10-14"`
    """
    new_agegroups = list()

    for agegroup in agegroups:
        # check if agegroup already exist
        agegroup_exists = get_agegroup05y(session=session, agegroup=agegroup)
        if agegroup_exists is not None:
            continue

        # create new agegroup
        entry = tbl.Agegroup05y(agegroup=agegroup)
        new_agegroups.append(entry)

    # add hardcoded special agegroups
    #_EIGHTYPLUS = tbl.Agegroup05y(agegroup="80+")
    #_UNK = tbl.Agegroup05y(agegroup="UNK")

    #if get_agegroup05y(session=session, agegroup="80+") is None:
    #    new_agegroups.append(_EIGHTYPLUS)
    #if get_agegroup05y(session=session, agegroup="UNK") is None:
    #    new_agegroups.append(_UNK)

    session.add_all(new_agegroups)

def insert_agegroups10y(session: Session, agegroups: list[str]) -> None:
    """
    Adds a new agegroup with a 10-year-interval to the local SQLite database.
    Calculates average age.
    Args:
        agegroup: Agegroup with a 10-year interval. Example: `"00-09"` or `"10-19"`
    """
    new_agegroups = list()

    for agegroup in agegroups:
        if agegroup == "80+":
            new_agegroup = tbl.Agegroup10y(
                agegroup="80+", 
                number_observations=21, 
                avg_age=90.0
            )
            if get_agegroup10y(session=session, agegroup="80+") is None:
                new_agegroups.append(new_agegroup)
            continue

        if agegroup == "UNK":
            new_agegroup = tbl.Agegroup10y(
                agegroup="UNK", 
                number_observations=0, 
                avg_age=0
            )
            if get_agegroup10y(session=session, agegroup="UNK") is None:
                new_agegroups.append(new_agegroup)
            continue

        assert agegroup.__contains__("-"), "Agegroup must contain delimiter '-'"

        # check if agegroup already exist
        agegroup_exists = get_agegroup10y(session=session, agegroup=agegroup)
        if agegroup_exists is not None:
            continue

        n_observations = (_get_max_age(agegroup) - _get_min_age(agegroup)) + 1
        # create new agegroup
        entry = tbl.Agegroup10y(
            agegroup=agegroup,
            number_observations=n_observations,
            avg_age=_calc_avg_age(agegroup, n_observations),
        )
        new_agegroups.append(entry)

    # write to DB
    session.add_all(new_agegroups)


def insert_agegroupsRki(session: Session, agegroups: list[str]) -> None:
    """
    Adds a new agegroup with a year-interval defined by RKI (Robert-Koch_Institut).
    Calculates average age.
    Args:
        agegroup: Agegroup with the RKI-standard interval
    """
    new_agegroups = list()

    for agegroup in agegroups:
        if agegroup == "80+":
            new_agegroup = tbl.AgegroupRki(
                agegroup="80+", 
                number_observations=21, 
                avg_age=90.0
            )
            if get_agegroup_rki(session=session, agegroup="80+") is None:
                new_agegroups.append(new_agegroup)
            continue

        if agegroup == "UNK":
            new_agegroup = tbl.AgegroupRki(
                agegroup="UNK", 
                number_observations=0, 
                avg_age=0
            )
            if get_agegroup_rki(session=session, agegroup="UNK") is None:
                new_agegroups.append(new_agegroup)
            continue

        assert agegroup.__contains__("-"), "Agegroup must contain delimiter '-'"

        # check if agegroup already exist
        agegroup_exists = get_agegroup_rki(session=session, agegroup=agegroup)
        if agegroup_exists is not None:
            continue

        n_observations = (_get_max_age(agegroup) - _get_min_age(agegroup)) + 1
        # create new agegroup
        entry = tbl.AgegroupRki(
            agegroup=agegroup,
            number_observations=n_observations,
            avg_age=_calc_avg_age(agegroup, n_observations),
        )
        new_agegroups.append(entry)

    # write to DB
    session.add_all(new_agegroups)
