# Author: Daniel Stöcklein

import pandas as pd
from src.mysql_db import db_helper as database


def weekly_deaths(insert_into: str, country_code: str):
    db_raw = database.RawDB()
    db_proj = database.ProjDB()

    # get weekly deaths total
    df = pd.DataFrame(
        db_raw.get_oecd_weekly_deaths_total(country_code=country_code)
    )

    # leading zeros
    df['week'] = df['week'].astype(str).str.zfill(2)

    # create ISO-KEY
    df['iso_key'] = df['year'].astype(str) + df['week']
    df['iso_key'] = df['iso_key'].astype(int)

    # merge calendar_yr foreign key
    df = db_proj.merge_fk(df,
                          table='calendar_cw',
                          df_fk='iso_key',
                          table_fk='iso_key',
                          drop_columns=['iso_key', 'calendar_yr_id', 'iso_cw']
                          )

    df.rename(
        columns={'value': 'deaths'},
        inplace=True
    )

    db_proj.insert_and_append(df, insert_into)

    db_raw.db_close()
    db_proj.db_close()


def weekly_covid_deaths(insert_into: str, country_code: str):
    db_raw = database.RawDB()
    db_proj = database.ProjDB()

    # get weekly deaths total
    df = pd.DataFrame(
        db_raw.get_oecd_weekly_covid_deaths_total(country_code=country_code)
    )

    # leading zeros
    df['week'] = df['week'].astype(str).str.zfill(2)

    # create ISO-KEY
    df['iso_key'] = df['year'].astype(str) + df['week']
    df['iso_key'] = df['iso_key'].astype(int)

    # merge calendar_yr foreign key
    df = db_proj.merge_fk(df,
                          table='calendar_cw',
                          df_fk='iso_key',
                          table_fk='iso_key',
                          drop_columns=['iso_key', 'calendar_yr_id', 'iso_cw']
                          )

    df.rename(
        columns={'value': 'deaths'},
        inplace=True
    )

    db_proj.insert_and_append(df, insert_into)

    db_raw.db_close()
    db_proj.db_close()
