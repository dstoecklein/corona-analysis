import pandas as pd
import eurostat
from src.database import db_helper as database
from src.utils import estat_helper

if __name__ == '__main__':
    db_proj = database.ProjDB()

    df = eurostat.get_data_df(
        'demo_r_d2jan',
        flags=False
    )

    # clearing some usual eurostat stuff
    df = estat_helper.clear_estat_data(df)

    df = df.query(
        '''
        geo == @countries & geo != 'DE_TOT' \
        & age != 'TOTAL' & age !='Y_GE75' & age != 'Y80-84' & age != 'Y_GE85' \
        & sex == 'T' 
        '''
    )

    df.to_csv("test.csv", sep=";")

def population_by_agegroups(table: str, countries: list, starting_year=2010):
    # Datasource: Eurostat

    db_proj = database.ProjDB()

    df = eurostat.get_data_df(
        'demo_pjangroup',
        flags=False
    )

    # clearing some usual eurostat stuff
    df = estat_helper.clear_estat_data(df)

    df = df.query(
        '''
        geo == @countries & geo != 'DE_TOT' \
        & age != 'TOTAL' & age !='Y_GE75' & age != 'Y80-84' & age != 'Y_GE85' \
        & sex == 'T' 
        '''
    )

    # melting to years
    df = df.melt(
        id_vars=['age', 'sex', 'unit', 'geo'],
        var_name='year',
        value_name='population'
    )

    # assign 10-year agegroups
    df = df.assign(
        agegroup_10y=df['age'].map(
            estat_helper.ANNUAL_POPULATION_AGEGROUP_10Y_MAP
        )
    ).fillna('UNK')

    df['year'] = pd.to_numeric(df['year'], errors='coerce')

    # only population from year 2010 - today
    df = df[df['year'] >= starting_year]

    # merge foreign keys
    df = db_proj.merge_agegroups_fk(df, left_on='agegroup_10y', interval='10y')
    df = db_proj.merge_calendar_years_fk(df, left_on='year')
    df = db_proj.merge_countries_fk(df, left_on='geo', iso_code='alpha2')

    del df['age']
    del df['geo']
    del df['sex']
    del df['unit']

    df = df.groupby(
        [
            'countries_fk',
            'calendar_years_fk',
            'agegroups_10y_fk'
        ], as_index=False
    )['population'].sum()

    db_proj.insert_or_update(df, table)

    db_proj.db_close()
