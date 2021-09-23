# Author: Daniel Stöcklein

import pandas as pd
from src.mysql_db import db_helper
from src.utils import paths
from src.web_scraper import web_scrap_helper

URL_MORTALITY = 'https://stats.oecd.org/SDMX-JSON/data/HEALTH_MORTALITY?contentType=csv'

PATH = paths.get_covid19_ger_path()


def weekly_deaths(insert_into: str):
    db = db_helper.RawDB()

    df = pd.read_csv(
        web_scrap_helper.http_request(URL_MORTALITY),
        engine='python',
        sep=','
    )

    df.fillna(0, inplace=True)

    df.columns = df.columns.str.lower()
    df = df.loc[:, ~df.columns.duplicated()]  # remove duplicate columns
    del df['week number']
    del df['flag codes']

    db.insert_and_replace(df, insert_into)
    db.db_close()
