from table_helpers import countries_helper, calendar_helper
from database.db import Database
from config.core import cfg_db

database = Database(db_uri=f"{cfg_db.db.dialect}{cfg_db.db.name}.db")

with database.ManagedSessionMaker() as session:
    y = calendar_helper.get_calendar_days(session, 2010)
    print(y)
    print(type(y))
    for k in y:
        print(k)
