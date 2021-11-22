import pandas as pd
import eurostat
from src.database import db_helper as database
from src.utils import estat_helper


def population_by_agegroups(table: str, countries: list, starting_year: int = 1990):
    # Datasource: Eurostat

    db_proj = database.ProjDB()

    tmp = eurostat.get_data_df(
        'demo_pjangroup',
        flags=False
    ).copy()

    # clearing some usual eurostat stuff
    tmp = estat_helper.clear_estat_data(tmp)

    if countries:
        tmp = tmp.query(
            '''
            geo == @countries \
            & age != 'TOTAL' & age !='Y_GE75' & age != 'Y80-84' & age != 'Y_GE85' \
            & sex == 'T' 
            '''
        )
    else:
        tmp = tmp.query(
            '''
            geo.str.len() == 2 \
            & age != 'TOTAL' & age !='Y_GE75' & age != 'Y80-84' & age != 'Y_GE85' \
            & sex == 'T' 
            '''
        )

    # melting to years
    tmp = tmp.melt(
        id_vars=['age', 'sex', 'unit', 'geo'],
        var_name='year',
        value_name='population'
    )

    # assign 10-year agegroups
    tmp = tmp.assign(
        agegroup_10y=tmp['age'].map(
            estat_helper.ANNUAL_POPULATION_AGEGROUP_10Y_MAP
        )
    ).fillna('UNK')

    tmp['year'] = pd.to_numeric(tmp['year'], errors='coerce')

    # only population from year 2010 - today
    tmp = tmp[tmp['year'] >= starting_year]

    # merge foreign keys
    tmp = db_proj.merge_agegroups_fk(tmp, left_on='agegroup_10y', interval='10y')
    tmp = db_proj.merge_calendar_years_fk(tmp, left_on='year')
    tmp = db_proj.merge_countries_fk(tmp, left_on='geo', country_code='iso_3166_1_alpha2')

    del tmp['age']
    del tmp['geo']
    del tmp['sex']
    del tmp['unit']

    tmp = tmp.groupby(
        [
            'countries_fk',
            'calendar_years_fk',
            'agegroups_10y_fk'
        ], as_index=False
    )['population'].sum()


    db_proj.insert_or_update(tmp, table)

    db_proj.db_close()
