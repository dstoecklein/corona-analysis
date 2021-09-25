# Author: Daniel Stöcklein

import pandas as pd
from src.database import db_helper as database


def daily_covid(insert_into: str, country_code: str):

    # create db connection
    db_raw = database.RawDB()
    db_proj = database.ProjDB()

    # owid covid data
    tmp = db_raw.get_owid_daily_covid(country_code=country_code)

    # convert to date
    tmp['date'] = pd.to_datetime(tmp['date'], infer_datetime_format=True)

    cols = [
        'date',
        'total_cases',
        'new_cases',
        'new_cases_smoothed',
        'total_deaths',
        'new_deaths',
        'new_deaths_smoothed',
        'reproduction_rate',
        'icu_patients',
        'hosp_patients',
        'total_tests',
        'new_tests',
        'new_tests_smoothed',
        'positive_rate',
        'tests_per_case',
        'total_vaccinations',
        'new_vaccinations',
        'new_vaccinations_smoothed',
        'people_vaccinated',
        'people_fully_vaccinated',
        'total_boosters',
        'stringency_index',
        'population',
        'median_age',
        'life_expectancy'
    ]

    tmp = tmp[cols]

    new_cols = [
        'reporting_date',
        'cases',
        'cases_delta',
        'cases_delta_smoothed',
        'deaths',
        'deaths_delta',
        'deaths_delta_smoothed',
        'rvalue',
        'icu_patients',
        'hosp_patients',
        'tests',
        'tests_delta',
        'tests_delta_smoothed',
        'positive_rate',
        'tests_per_case',
        'vaccinations',
        'vaccinations_delta',
        'vaccinations_delta_smoothed',
        'people_vaccinated',
        'people_fully_vaccinated',
        'total_boosters',
        'stringency_index',
        'population',
        'median_age',
        'life_expectancy'
    ]

    tmp.rename(
        columns=dict(zip(tmp.columns, new_cols)),
        inplace=True
    )

    # owid doesnt provide this information!?
    tmp['recovered'] = 0
    tmp['active_cases'] = 0

    # create iso key
    tmp = tmp.assign(iso_key=tmp['reporting_date'].dt.strftime('%G%V').astype(int))

    # merge calendar_yr foreign key
    tmp = db_proj.merge_fk(tmp,
                           table='calendar_cw',
                           df_fk='iso_key',
                           table_fk='iso_key',
                           drop_columns=['iso_key', 'calendar_yr_id', 'iso_cw']
                           )

    # insert only new rows
    db_proj.insert_and_append(tmp, insert_into)

    db_raw.db_close()
    db_proj.db_close()
