import uuid
import pandas as pd
from sqlalchemy import Table
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.engine.row import Row
from sqlalchemy.orm import Session

from db_helper2 import Database
import create_tables as tbl


DB = Database()



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
    with session.begin():
        session.add_all(new_years)
        session.commit()

