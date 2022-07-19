from table_helpers import countries_helper, calendars_helper
from database.db import Database
from config.core import cfg_db
import table_helpers
import pandas as pd

database = Database(db_uri=f"{cfg_db.db.dialect}{cfg_db.db.name}.db")
import database.tables as tbl


with database.ManagedSessionMaker() as session:
    

    df = calendars_helper.get_calendar_weeks_df(session)

    print(df)