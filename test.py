import database.tables as tbl
#from database.db2 import Store
from config.core import cfg_db, cfg_init_values
import table_helpers.agegroups_helper as ag
from table_helpers import icd10_helper

from database.db import Database
DB = Database(f"{cfg_db.dialect}{cfg_db.name}.db")
import pandas as pd


with DB.ManagedSessionMaker() as session:
    with session.begin():
        df = pd.read_csv("files/icd102.csv", encoding="utf8", index_col="icd10")
        icd10_dict = df.to_dict(orient="index")
        icd10_helper.add_new_icd10(session, icd10_dict)

#init_main(DB)

