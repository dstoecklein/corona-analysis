# Author: Daniel Stöcklein

import pandas as pd
import numpy as np
from src.mysql_db import db_helper as database


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


def annual_deaths_causes(insert_into: str, country_code: str):
    db_raw = database.RawDB()
    db_proj = database.ProjDB()

    # get weekly deaths
    df = db_raw.get_estat_annual_death_causes(country_code=country_code)

    db_raw.db_close()

    # change 5y-agegroup to 10y-agegroup
    df.loc[df['age'].str.contains('Y_LT1'), 'age'] = '0'
    df.loc[df['age'].str.contains('Y1-4'), 'age'] = '0'
    df.loc[df['age'].str.contains('Y5-9'), 'age'] = '0'

    df.loc[df['age'].str.contains('Y10-14'), 'age'] = '10'
    df.loc[df['age'].str.contains('Y15-19'), 'age'] = '10'

    df.loc[df['age'].str.contains('Y20-24'), 'age'] = '20'
    df.loc[df['age'].str.contains('Y25-29'), 'age'] = '20'

    df.loc[df['age'].str.contains('Y30-34'), 'age'] = '30'
    df.loc[df['age'].str.contains('Y35-39'), 'age'] = '30'

    df.loc[df['age'].str.contains('Y40-44'), 'age'] = '40'
    df.loc[df['age'].str.contains('Y45-49'), 'age'] = '40'

    df.loc[df['age'].str.contains('Y50-54'), 'age'] = '50'
    df.loc[df['age'].str.contains('Y55-59'), 'age'] = '50'

    df.loc[df['age'].str.contains('Y60-64'), 'age'] = '60'
    df.loc[df['age'].str.contains('Y65-69'), 'age'] = '60'

    df.loc[df['age'].str.contains('Y70-74'), 'age'] = '70'
    df.loc[df['age'].str.contains('Y75-79'), 'age'] = '70'

    df.loc[df['age'].str.contains('Y80-84'), 'age'] = '80'
    df.loc[df['age'].str.contains('Y85-89'), 'age'] = '80'
    df.loc[df['age'].str.contains('Y90-94'), 'age'] = '80'
    df.loc[df['age'].str.contains('Y_GE95'), 'age'] = '80'

    # create 10-year-agegroups
    df['age'] = df['age'].astype(int)
    bins = [0, 10, 20, 30, 40, 50, 60, 70, 80, 999]
    labels = ['0-9', '10-19', '20-29', '30-39', '40-49', '50-59', '60-69', '70-79', '80+']
    df['agegroup_10y'] = pd.cut(
        df['age'],
        bins,
        labels=labels,
        right=False,
        include_lowest=True
    )

    # remove not needed columns
    del df['sex']
    del df['geo']
    del df['unit']

    df['year'] = df['year'].astype(int)

    # merge calendar_yr foreign key
    df = db_proj.merge_fk(df,
                          table='calendar_yr',
                          df_fk='year',
                          table_fk='iso_year',
                          drop_columns=['iso_year', 'year']
                          )

    # false icd10 categories
    df.loc[df['icd10'].str.contains('K72-K75'), 'icd10'] = 'K71-K77'
    df.loc[df['icd10'].str.contains('B180-B182'), 'icd10'] = 'B171-B182'

    # merge icd10 foreign key
    df = db_proj.merge_fk(df,
                          table='icd10_codes',
                          df_fk='icd10',
                          table_fk='icd10',
                          drop_columns=['icd10', 'description_eng', 'description_ger']
                          )
    # rest unkown
    df['icd10_codes_id'] = df['icd10_codes_id'].fillna(value=386).astype(int)

    # merge agegroup foreign key
    df = db_proj.merge_fk(df,
                          table='agegroups_10y',
                          df_fk='agegroup_10y',
                          table_fk='agegroup',
                          drop_columns=['age', 'agegroup', 'agegroup_10y']
                          )

    db_proj.insert_and_append(df, insert_into)

    db_raw.db_close()
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
