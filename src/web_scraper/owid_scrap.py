# Author: Daniel Stöcklein

import pandas as pd
from src.database import db_helper
from src.utils import paths, web_scrap_helper

# constants
URL_COVID = 'https://covid.ourworldindata.org/data/owid-covid-data.csv'

PATH = paths.get_covid19_ger_path()


def daily_covid(insert_into: str):
    db = db_helper.RawDB()

    df = pd.read_csv(
        web_scrap_helper.http_request(URL_COVID),
        engine='python',
        sep=','
    )

    df.fillna(0, inplace=True)

    db.insert_and_replace(df, insert_into)
    db.db_close()
