import eurostat
import pandas as pd
from src.database import db_helper as database
from src.utils import estat_helper


def weekly_deaths(insert_into: str, countries: list, starting_year=2010):
    # Datasource: Eurostat

    db_proj = database.ProjDB()

    df = eurostat.get_data_df(
        'demo_r_mwk_10',
        flags=False
    )

    # clearing some usual eurostat stuff
    df = estat_helper.clear_estat_data(df)

    # filtering only needed data
    df = df.query(
        '''
        geo == @countries \
        & age != 'TOTAL' & age != 'Y80-89' & age != 'Y_GE90' \
        & sex == 'T' 
        '''
    )

    df = df.melt(
        id_vars=['age', 'sex', 'unit', 'geo'],
        var_name='year',
        value_name='deaths'
    )

    # split by W
    df[['iso_year', 'iso_cw']] = df['year'].str.split('W', expand=True)
    # leading zeros
    df['iso_cw'] = df['iso_cw'].str.zfill(2)

    # create ISO-KEY
    df['iso_key'] = df['iso_year'] + df['iso_cw']
    df['iso_year'] = pd.to_numeric(df['iso_year'], errors='coerce')
    df['iso_key'] = pd.to_numeric(df['iso_key'], errors='coerce')

    # remove week 99 rows
    df = df[df['iso_cw'] != 99]

    # selection from year
    df = df[df['iso_year'] >= starting_year]

    # assign 10-year agegroups
    df = df.assign(
        agegroup_10y=df['age'].map(
            estat_helper.WEEKLY_DEATHS_AGEGROUP_10Y_MAP
        )
    ).fillna('UNK')

    # merge foreign keys
    df = db_proj.merge_agegroups_fk(df, left_on='agegroup_10y', interval='10y')
    df = db_proj.merge_calendar_weeks_fk(df, left_on='iso_key')
    df = db_proj.merge_countries_fk(df, left_on='geo', iso_code='alpha2')

    # remove not needed columns
    del df['age']
    del df['geo']
    del df['sex']
    del df['unit']
    del df['year']
    del df['iso_year']
    del df['iso_cw']

    db_proj.insert_or_update(df, insert_into)

    db_proj.db_close()