# Author: Daniel Stöcklein

import pandas as pd
from src.mysql_db import db_helper as database
from src.utils import paths

PATH = paths.get_utils_path()
FILE = 'icd10-2007.csv'
TABLE = 'icd10_codes'

df = pd.read_csv(PATH + FILE, sep=';', encoding='utf8')

db = database.ProjDB()

db.insert_and_append(df, TABLE)

db.db_close()
