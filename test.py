from table_helpers import countries_helper
from database.db import Database
from config.core import cfg_db

database = Database(db_uri=f"{cfg_db.db.dialect}{cfg_db.db.name}.db")

with database.ManagedSessionMaker() as session:
    with session.begin():
        d = countries_helper.get_subdivision3(session, ags=9363)
        print(d)
        print(type(d))
        print(d.get("country_en"))

