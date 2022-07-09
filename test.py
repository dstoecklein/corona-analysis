from table_helpers import countries_helper, calendars_helper
from database.db import Database
from config.core import cfg_db
import table_helpers

database = Database(db_uri=f"{cfg_db.db.dialect}{cfg_db.db.name}.db")



with database.ManagedSessionMaker() as session:
    u = database.get_unique_cols("_agegroup_05y")
    print(u)
    #c = countries_helper.get_subdivision1(session=session, nuts_1="BE1")
    #rint(type(c))
    #print(type(c._mapping))
    #d = calendar_helper.get_calendar_weeks(session, 2051)
    #print(d)




"""
    y = calendar_helper.get_calendar_year(session, 2050)
    print(y.iso_year)
    
    y = calendar_helper.check_iso_key_exist(session, 205053)
    
    
    """
    #print(y[0])
