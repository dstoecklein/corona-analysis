# Author: Daniel Stöcklein
# TODO: Get data from Genesis API

import pandas as pd
from src.mysql_db import db_helper
from src.utils import paths

FILE = 'destatis_population_ger_states.csv'

PATH = paths.get_population_path()


def annual_population(insert_into: str):
    db_proj = db_helper.ProjDB()

    df = pd.read_csv(
        PATH +
        FILE,
        engine='python',
        sep=';',
        encoding='ISO-8859-1'
    )

    # change to date type and get year
    df = df.rename(columns={'Stichtag': 'iso_year'})
    df['iso_year'] = pd.to_datetime(df['iso_year'])
    df['iso_year'] = pd.DatetimeIndex(df['iso_year']).year.astype(int)

    # transpose row 'stichtag' to columns
    df = (df.set_index(["Alter", "iso_year"])
          .stack()
          .unstack("iso_year")
          .reset_index()
          .rename(columns={"level_1": "state"})
          )

    # create 10-year-agegroups
    bins = [0, 10, 20, 30, 40, 50, 60, 70, 80, 999]
    labels = ['0-9', '10-19', '20-29', '30-39', '40-49', '50-59', '60-69', '70-79', '80+']
    df['agegroup_10y'] = pd.cut(
        df['Alter'],
        bins,
        labels=labels,
        right=False,
        include_lowest=True
    )

    # group and sum
    df = df.groupby(
        ['agegroup_10y', 'state'],
        as_index=False
    ).sum()

    del df['Alter']

    df = pd.melt(
        df,
        id_vars=('state', 'agegroup_10y'),
        value_name='population'
    )

    df['iso_year'] = df['iso_year'].astype(int)

    # merge agegroup foreign key
    df = db_proj.merge_fk(df,
                          table='agegroups_10y',
                          df_fk='agegroup_10y',
                          table_fk='agegroup',
                          drop_columns=['agegroup', 'agegroup_10y']
                          )

    # merge calendar_yr foreign key
    df = db_proj.merge_fk(df,
                          table='calendar_yr',
                          df_fk='iso_year',
                          table_fk='iso_year',
                          drop_columns=['iso_year']
                          )

    # merge ger_states foreign key
    df = db_proj.merge_fk(df,
                          table='ger_states',
                          df_fk='state',
                          table_fk='state',
                          drop_columns=['state']
                          )

    # reorder columns
    df = df[['agegroups_10y_id', 'calendar_yr_id', 'ger_states_id', 'population']]

    # insert into dbf
    db_proj.insert_and_append(df, insert_into)

    db_proj.db_close()
