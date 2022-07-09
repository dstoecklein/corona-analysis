from table_helpers import countries_helper, calendars_helper
from database.db import Database
from config.core import cfg_db
import table_helpers

database = Database(db_uri=f"{cfg_db.db.dialect}{cfg_db.db.name}.db")
import database.tables as tbl


with database.ManagedSessionMaker() as session:
    
    #c = countries_helper.get_subdivision1(session=session, nuts_1="BE1")
    #rint(type(c))
    #print(type(c._mapping))
    #d = calendar_helper.get_calendar_weeks(session, 2051)
    #print(d)
    print(tbl.MortalityWeeklyAgegroup.__table__.columns)


    entry = tbl.MortalityWeeklyAgegroup(
            country_fk = 6,
            calendar_week_fk = 1664,
            agegroup10y_fk = 10,
            deaths = 0,
    )
    session.add(entry)

    s = session.query(tbl.MortalityWeeklyAgegroup).first()
    print(s)


"""

    y = calendar_helper.get_calendar_year(session, 2050)
    print(y.iso_year)
    
    y = calendar_helper.check_iso_key_exist(session, 205053)
    
    
    """
    #print(y[0])
