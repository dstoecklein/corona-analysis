# Author: Daniel Stöcklein

import os
import pandas as pd
from src.database import db_helper as database
from src.utils import paths

PATH = paths.get_utils_path()
FILE = 'calendar_cws.xlsx'
TABLE = 'calendar_cw'

db = database.ProjDB()
df = pd.read_excel(PATH + FILE)

# merge calendar_yr foreign key
df = db.merge_fk(df,
                 table='calendar_yr',
                 df_fk='iso_year',
                 table_fk='iso_year',
                 drop_columns=['iso_year']
                 )

df['iso_key'] = pd.to_numeric(df['iso_key'], errors='coerce')

db.insert_and_append(df, TABLE)

db.db_close()
