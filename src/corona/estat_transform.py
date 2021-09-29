# Author: Daniel Stöcklein

import pandas as pd
import numpy as np
import eurostat
from src.database import db_helper as database
from src.corona import estat_helper


def weekly_deaths(insert_into: str, country_code: str, iso_year: int):
    db_raw = database.RawDB()
    db_proj = database.ProjDB()

    # get weekly deaths
    df = db_raw.get_estat_weekly_deaths(country_code=country_code)

    # remove not needed columns
    del df['sex']
    del df['geo']
    del df['unit']

    # split by W
    df[['iso_year', 'iso_cw']] = df['year'].str.split('W', expand=True)

    # leading zeros
    df['iso_cw'] = df['iso_cw'].str.zfill(2)

    # create ISO-KEY
    df['iso_key'] = df['iso_year'] + df['iso_cw']

    df['iso_year'] = pd.to_numeric(df['iso_year'], errors='coerce')
    df['iso_key'] = pd.to_numeric(df['iso_key'], errors='coerce')

    # remove not needed rows
    df = df[df['iso_cw'] != 99]

    # only from year
    df = df[df['iso_year'] >= iso_year]

    # remove not needed columns
    del df['year']
    del df['iso_year']
    del df['iso_cw']

    df['age'] = df['age'].str.replace('Y_LT10', '0-9')
    df['age'] = df['age'].str.replace('Y_GE80', '80+')
    df['age'] = df['age'].str.replace('Y', '')

    df.rename(
        columns={'age': 'agegroup_10y'},
        inplace=True
    )

    # merge agegroup foreign key
    df = db_proj.merge_fk(df,
                          table='agegroups_10y',
                          df_fk='agegroup_10y',
                          table_fk='agegroup',
                          drop_columns=['agegroup', 'agegroup_10y']
                          )

    # merge calendar_yr foreign key
    df = db_proj.merge_fk(df,
                          table='calendar_cw',
                          df_fk='iso_key',
                          table_fk='iso_key',
                          drop_columns=['iso_key', 'calendar_yr_id', 'iso_cw']
                          )

    db_proj.insert_and_append(df, insert_into)

    db_raw.db_close()
    db_proj.db_close()


def annual_death_causes(insert_into: str, countries: list):
    db_proj = database.ProjDB()

    df = eurostat.get_data_df(
        'hlth_cd_aro',
        flags=False
    )

    # clearing some usual estat stuff
    df = estat_helper.clear_estat_data(df)

    # filtering only needed data
    df = df.query(
        '''
        geo == @countries \
        & age != 'TOTAL' & age !='Y_LT15' & age != 'Y15-24' & age != 'Y_LT25' & age != 'Y_LT65' \
        & age != 'Y_GE65' & age != 'Y_GE85' \
        & sex == 'T' \
        & resid == 'TOT_IN' \
        & icd10 != 'A-R_V-Y'
        '''
    )

    # melting to years
    df = df.melt(
        id_vars=['age', 'sex', 'unit', 'geo', 'icd10', 'resid'],
        var_name='year',
        value_name='deaths'
    )

    # assign 10-year agegroups
    df = df.assign(
        agegroup_10y=df['age'].map(
            estat_helper.AGEGROUP_10Y_MAP
        )
    )

    # false icd10 categories
    df.loc[df['icd10'].str.contains('K72-K75'), 'icd10'] = 'K71-K77'
    df.loc[df['icd10'].str.contains('B180-B182'), 'icd10'] = 'B171-B182'

    # merge foreign keys
    df = db_proj.merge_calendar_years_fk(df, left_on='year')
    df = db_proj.merge_classifications_icd10_fk(df, left_on='icd10')
    df = db_proj.merge_agegroups_fk(df, left_on='agegroup_10y', interval='10y')
    df = db_proj.merge_countries_fk(df, left_on='geo', iso_code='alpha2')

    # if icd10 n/a, give them 'unkown' foreign key
    df['classifications_icd10_fk'] = df['classifications_icd10_fk'].fillna(value=386).astype(int)

    del df['age']
    del df['sex']
    del df['unit']
    del df['resid']

    df = df.groupby(
        [
            'classifications_icd10_fk',
            'agegroups_10y_fk',
            'countries_fk',
            'calendar_years_fk'
        ], as_index=False
    )['deaths'].sum()

    db_proj.insert_and_append(df, insert_into)

    db_proj.db_close()


def annual_population(insert_into: str, country_code: str):
    db_raw = database.RawDB()
    db_proj = database.ProjDB()

    # get population
    df = db_raw.get_estat_annual_population(country_code=country_code)

    del df['unit']
    del df['sex']
    del df['geo']

    df = df.melt(
        id_vars=['age'],
        var_name='year',
        value_name='population'
    )

    # create labels to merge in 10year groups
    conditions = [
        (df['age'] == 'Y_LT5') | (df['age'] == 'Y5-9'),
        (df['age'] == 'Y10-14') | (df['age'] == 'Y15-19'),
        (df['age'] == 'Y20-24') | (df['age'] == 'Y25-29'),
        (df['age'] == 'Y30-34') | (df['age'] == 'Y35-39'),
        (df['age'] == 'Y40-44') | (df['age'] == 'Y45-49'),
        (df['age'] == 'Y50-54') | (df['age'] == 'Y55-59'),
        (df['age'] == 'Y60-64') | (df['age'] == 'Y65-69'),
        (df['age'] == 'Y70-74') | (df['age'] == 'Y75-79'),
        (df['age'] == 'Y80-84') | (df['age'] == 'Y_GE85')
    ]
    labels = ["0-9", "10-19", "20-29", "30-39", "40-49", "50-59", "60-69", "70-79", "80+"]
    df['agegroup_10y'] = np.select(conditions, labels, default=-1)

    del df['age']

    # group by agegroups
    df = df.groupby(
        ['year', 'agegroup_10y']
    ).sum().reset_index()

    df.rename(
        columns={'year': 'iso_year'},
        inplace=True
    )

    df['iso_year'] = pd.to_numeric(df['iso_year'], errors='coerce')

    # only population from year 2010 - today
    df = df[df['iso_year'] >= 2010]

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

    # reorder columns
    df = df[['agegroups_10y_id', 'calendar_yr_id', 'population']]

    # insert into dbf
    db_proj.insert_and_append(df, insert_into)

    db_raw.db_close()
    db_proj.db_close()
