import database.tables as tbl
#from database.db2 import Store
from config.core import cfg_db, cfg_init_values
import table_helpers.agegroups_helper as ag


from database.db import Database
DB = Database(f"{cfg_db.dialect}{cfg_db.name}.db")

from init_database import main as init_main
dd = DB._db_uri_sql_alchemy_engine_map

for k, v in dd.items():
    print(type(k))
    print(type(v))
#init_main(DB)

