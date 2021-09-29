# Author: Daniel Stöcklein

import pandas as pd
import numpy as np
import eurostat
from src.database import db_helper as database


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


def clear_data(df: pd.DataFrame):
    for i in df.columns:
        if i not in ('age', 'sex', 'unit', 'geo\\time', 'icd10', 'resid'):
            df[i] = df[i].fillna(0)
            df[i] = df[i].astype(int)
    df.rename(columns={'geo\\time': 'geo'}, inplace=True)
    return df


def annual_deaths_causes(insert_into: str, countries: list):
    db_proj = database.ProjDB()

    df = eurostat.get_data_df(
        'hlth_cd_aro',
        flags=False
    )

    df = clear_data(df)

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

    df = df.melt(
        id_vars=['age', 'sex', 'unit', 'geo', 'icd10', 'resid'],
        var_name='year',
        value_name='deaths'
    )

    agegroup_10y_map = {
        'Y_LT1': '00-09', 'Y1-4': '00-09', 'Y5-9': '00-09',
        'Y10-14': '10-19', 'Y15-19': '10-19',
        'Y20-24': '20-29', 'Y25-29': '20-29',
        'Y30-34': '30-39', 'Y35-39': '30-39',
        'Y40-44': '40-49', 'Y45-49': '40-49',
        'Y50-54': '50-59', 'Y55-59': '50-59',
        'Y60-64': '60-69', 'Y65-69': '60-69',
        'Y70-74': '70-79', 'Y75-79': '70-79',
        'Y80-84': '80+', 'Y85-89': '80+', 'Y90-94': '80+', 'Y_GE95': '80+'
    }

    df = df.assign(agegroup_10y=df['age'].map(agegroup_10y_map))

    # remove not needed columns
    del df['sex']
    del df['unit']

    df['year'] = df['year'].astype(int)

    # merge calendar_yr foreign key
    df = db_proj.merge_fk(df,
                          table='_calendar_years',
                          df_fk='year',
                          table_fk='iso_year',
                          drop_columns=['iso_year', 'year']
                          )
    df.rename(
        columns={'ID': 'calendar_years_fk'},
        inplace=True
    )

    # false icd10 categories
    df.loc[df['icd10'].str.contains('K72-K75'), 'icd10'] = 'K71-K77'
    df.loc[df['icd10'].str.contains('B180-B182'), 'icd10'] = 'B171-B182'

    # merge icd10 foreign key
    df = db_proj.merge_fk(df,
                          table='_classifications_icd10',
                          df_fk='icd10',
                          table_fk='icd10',
                          drop_columns=['icd10', 'description_en', 'description_de']
                          )

    df.rename(
        columns={'ID': 'classifications_icd10_fk'},
        inplace=True
    )

    # rest unkown
    df['classifications_icd10_fk'] = df['classifications_icd10_fk'].fillna(value=386).astype(int)

    del df['last_update']
    del df['resid']
    del df['age']

    df = df.groupby(
        [
            'classifications_icd10_fk',
            'geo',
            'agegroup_10y',
            'calendar_years_fk'
        ], as_index=False
    )['deaths'].sum()

    # merge agegroup foreign key
    df = db_proj.merge_fk(df,
                          table='_agegroups_10y',
                          df_fk='agegroup_10y',
                          table_fk='agegroup',
                          drop_columns=['agegroup_10y', 'agegroup']
                          )
    df.rename(
        columns={'ID': 'agegroups_10y_fk'},
        inplace=True
    )

    df['geo'] = df['geo'].str.lower()

    # merge countries foreign key
    df = db_proj.merge_fk(df,
                          table='_countries',
                          df_fk='geo',
                          table_fk='iso_3166_alpha2',
                          drop_columns=['geo', 'country_en', 'country_de', 'latitude', 'longitude', 'iso_3166_alpha2',
                                        'iso_3166_alpha3', 'iso_3166_numeric']
                          )
    df.rename(
        columns={'ID': 'countries_fk'},
        inplace=True
    )

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
