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
            """
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
