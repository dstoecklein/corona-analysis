from db_helper2 import Database
import create_tables as tbl

DB = Database()


def get_agegroups_05y() -> list[tbl.Agegroups_05y]:
    """
    Get a list of agegroup objects with a 05-year interval

    Returns:
        A `list` of `Agegroups_05y` objects
    """
    session = DB.create_session()
    return session.query(tbl.Agegroups_05y).order_by(tbl.Agegroups_05y.agegroup).all()


def get_agegroups_10y() -> list[tbl.Agegroups_10y]:
    """
    Get a list of agegroup objects with a 10-year interval

    Returns:
        A `list` of `Agegroups_10y` objects
    """
    session = DB.create_session()
    return session.query(tbl.Agegroups_10y).order_by(tbl.Agegroups_10y.agegroup).all()


def add_new_agegroup_05y(agegroup: str) -> None:
    """
    Adds a new agegroup with a 5-year-interval to the local SQLite database.

    Args:
        agegroup: Agegroup with a 5-year interval. Example: `"00-04"` or `"10-14"`
    """

    session = DB.create_session()

    # check if agegroup already exist
    new_agegroup = (
        session.query(tbl.Agegroups_05y)
        .filter(tbl.Agegroups_05y.agegroup == agegroup)
    ).one_or_none()

    if new_agegroup is not None:
        return
        
    # create new agegroup
    new_agegroup = tbl.Agegroups_05y(
        agegroup=agegroup,
        unique_key=agegroup
    )

    # write to DB
    try:
        session.add(new_agegroup)
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def add_new_agegroup_10y(agegroup: str, avg_age: float, number_observations: int = 10) -> None:
    """
    Adds a new agegroup with a 10-year-interval to the local SQLite database.

    Args:
        agegroup: Agegroup with a 10-year interval. Example: `"00-09"` or `"10-19"`
        avg_age: Average age for the provided `agegroup`
        number_observations: Number of observations for the provided `agegroup`. 
        This is only necessary when an `agegroup` outside of the 10-year interval is provided.
        Defaults to `10`.
    """

    session = DB.create_session()
    
    # check if agegroup already exist
    new_agegroup = (
        session.query(tbl.Agegroups_10y)
        .filter(tbl.Agegroups_10y.agegroup == agegroup)
    ).one_or_none()

    if new_agegroup is not None:
        return
    
    # create new agegroup
    new_agegroup = tbl.Agegroups_10y(
        agegroup=agegroup, 
        number_observations=number_observations, 
        avg_age=avg_age,
        unique_key=agegroup
    )
    
    # write to DB
    try:
        session.add(new_agegroup)
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == "__main__":
    l = get_agegroups_10y()
    print(type(l))
    for i in l:
        print(i.agegroup)
    """
    agegroups_05y = [
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
    for item in agegroups_05y:
        add_new_agegroup_05y(agegroup=item)

    agegroups_10y = { # agegroup, [number_observations, avg_age]
        "00-09": [10, 4.5],
        "10-19": [10, 14.5],
        "20-29": [10, 24.5],
        "30-39": [10, 34.5],
        "40-49": [10, 44.5],
        "50-59": [10, 54.5],
        "60-69": [10, 64.5],
        "70-79": [10, 74.5],
        "80+": [21, 90],
        "UNK": [0, 0],
    }
    for k, v, in agegroups_10y.items():
        add_new_agegroup_10y(agegroup=k, number_observations=v[0], avg_age=v[1])
    """