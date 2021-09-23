# Author: Daniel Stöcklein

import eurostat
import pandas as pd
from src.mysql_db import db_helper


def weekly_deaths(insert_into: str):
    db = db_helper.RawDB()
    df = eurostat.get_data_df(
        'demo_r_mwk_10',
        flags=False
    )
    df = clear_data(df)
    df = df.melt(
        id_vars=['age', 'sex', 'unit', 'geo'],
        var_name='year',
        value_name='deaths'
    )
    db.insert_and_replace(df, insert_into)
    db.db_close()


def annual_death_causes(insert_into: str):
    db = db_helper.RawDB()
    df = eurostat.get_data_df(
        'hlth_cd_aro',
        flags=False
    )
    df = clear_data(df)
    df = df.melt(
        id_vars=['age', 'sex', 'unit', 'geo', 'icd10', 'resid'],
        var_name='year',
        value_name='deaths'
    )
    db.insert_and_replace(df, insert_into)
    db.db_close()


def annual_population(insert_into: str):
    db = db_helper.RawDB()
    df = eurostat.get_data_df(
        'demo_pjangroup',
        flags=False
    )
    df = clear_data(df)
    db.insert_and_replace(df, insert_into)
    db.db_close()


def clear_data(dataframe: pd.DataFrame):
    for i in dataframe.columns:
        if i not in ('age', 'sex', 'unit', 'geo\\time', 'icd10', 'resid'):
            dataframe[i] = dataframe[i].fillna(0)
            dataframe[i] = dataframe[i].astype(int)
    dataframe.rename(columns={'geo\\time': 'geo'}, inplace=True)
    return dataframe
