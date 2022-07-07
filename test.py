from table_helpers import countries_helper, calendar_helper
from database.db import Database
from config.core import cfg_db
import table_helpers

database = Database(db_uri=f"{cfg_db.db.dialect}{cfg_db.db.name}.db")



with database.ManagedSessionMaker() as session:
    calendar_helper.add_new_calendar_years2(session, [2051, 2052])

"""
    y = calendar_helper.get_calendar_year(session, 2050)
    print(y.iso_year)
    
    y = calendar_helper.check_iso_key_exist(session, 205053)
    
    
    """
    #print(y[0])
