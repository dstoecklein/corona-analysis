# Author: Daniel Stöcklein

import pandas as pd
import datetime as dt
from src.database import db_helper as database
from src.utils import paths
from src.web_scraper import rki_scrap

PATH = paths.get_covid19_ger_path()


def main():
    date = dt.datetime.now()
    df = rki_scrap.daily_covid(save_file=True)

    try:
        df['Meldedatum'] = pd.to_datetime(df['Meldedatum'], infer_datetime_format=True)

        if 'Refdatum' in df.columns:
            df['Refdatum'] = pd.to_datetime(df['Refdatum'], infer_datetime_format=True)
    except (KeyError, TypeError):
        print('Error trying to convert Date columns')

    # remove whitespaces from header
    df.columns = df.columns.str.replace(' ', '')

    daily_covid_cumulative(df, date, insert_into='rki_daily_covid_ger')
    #daily_covid_cumulative_agegroups(df, date, insert_into='rki_daily_covid_agegroups_ger')
    #daily_covid_cumulative_states(df, date, insert_into='rki_daily_covid_states_ger')
    #weekly_covid(df, df['Meldedatum'], insert_into='rki_weekly_covid_ger')


def daily_covid_cumulative(df: pd.DataFrame, date: dt.datetime, insert_into: str):
    # create db connection
    db = database.ProjDB()

    # get ger population
    population = db.get_destatis_annual_population(year='2020')

    # calculate rki corona numbers
    tmp = calc_numbers(df, date)
    tmp = tmp.groupby('reporting_date').sum().reset_index()

    # incidence 7 days
    tmp['incidence_7d'] = (tmp['cases_7d'] / population) * 100000
    tmp['incidence_7d_sympt'] = (tmp['cases_7d_sympt'] / population) * 100000
    tmp['incidence_7d_ref'] = (tmp['cases_7d_ref'] / population) * 100000
    tmp['incidence_7d_ref_sympt'] = (tmp['cases_7d_ref_sympt'] / population) * 100000

    # create iso key
    tmp = create_iso_key(tmp)

    # merge calendar_yr foreign key
    tmp = db.merge_fk(tmp,
                      table='calendar_cw',
                      df_fk='iso_key',
                      table_fk='iso_key',
                      drop_columns=['iso_key', 'calendar_yr_id', 'iso_cw']
                      )
    df.to_csv("test.csv", index=False)
    # insert only new rows
    #db.insert_only_new_rows(tmp, insert_into)

    db.db_close()

if __name__ == "__main__":
    main()

def daily_covid_cumulative_agegroups(df: pd.DataFrame, date: dt.datetime, insert_into: str):
    # create db connection
    db = database.ProjDB()

    # calculate rki corona numbers
    tmp = calc_numbers(df, date)
    tmp = tmp.groupby(['rki_agegroups', 'reporting_date']).sum().reset_index()

    # create iso key
    tmp = create_iso_key(tmp)

    # merge calendar_yr foreign key
    tmp = db.merge_fk(tmp,
                      table='calendar_cw',
                      df_fk='iso_key',
                      table_fk='iso_key',
                      drop_columns=['iso_key', 'calendar_yr_id', 'iso_cw']
                      )

    # insert only new rows
    db.insert_only_new_rows(tmp, insert_into)

    db.db_close()


def daily_covid_cumulative_states(df: pd.DataFrame, date: dt.datetime, insert_into: str):
    # create db connection
    db = database.ProjDB()

    # save states population in dataframe
    df_states_population = db.get_destatis_annual_population_states('2020')

    tmp = calc_numbers(df, date)

    # merge states population
    tmp = tmp.merge(df_states_population,
                    left_on='IdBundesland',
                    right_on='ger_states_id',
                    how='left',
                    )

    # incidence 7 days
    tmp['incidence_7d'] = (tmp['cases_7d'] / tmp['population']) * 100000
    tmp['incidence_7d_sympt'] = (tmp['cases_7d_sympt'] / tmp['population']) * 100000
    tmp['incidence_7d_ref'] = (tmp['cases_7d_ref'] / tmp['population']) * 100000
    tmp['incidence_7d_ref_sympt'] = (tmp['cases_7d_ref_sympt'] / tmp['population']) * 100000

    tmp = tmp.groupby(['ger_states_id', 'reporting_date']) \
        .sum() \
        .reset_index()

    # create iso key
    tmp = create_iso_key(tmp)

    # merge calendar_yr foreign key
    tmp = db.merge_fk(tmp,
                      table='calendar_cw',
                      df_fk='iso_key',
                      table_fk='iso_key',
                      drop_columns=['iso_key', 'calendar_yr_id', 'iso_cw']
                      )

    db.insert_only_new_rows(tmp, insert_into)

    db.db_close()


def weekly_covid(df: pd.DataFrame, date: dt.datetime, insert_into: str):
    # create db connection
    db = database.ProjDB()

    tmp = calc_numbers(df, date)

    # create iso key
    tmp = create_iso_key(tmp)

    # merge calendar_yr foreign key
    tmp = db.merge_fk(tmp,
                      table='calendar_cw',
                      df_fk='iso_key',
                      table_fk='iso_key',
                      drop_columns=['iso_key', 'calendar_yr_id', 'iso_cw']
                      )

    tmp = tmp.groupby('calendar_cw_id').sum().reset_index()

    # empty table and insert
    db.insert_and_append(tmp, insert_into)

    db.db_close()


def annual_covid(insert_into: str):
    # create db connection
    db = database.ProjDB()

    query = \
        '''
        SELECT * FROM rki_daily_covid_ger 
        INNER JOIN calendar_cw ON rki_daily_covid_ger.calendar_cw_id = calendar_cw.calendar_cw_id
        INNER JOIN calendar_yr ON calendar_yr.calendar_yr_id = calendar_cw.calendar_yr_id;
        '''

    # create dataframe
    df = pd.read_sql(query, db.connection)

    # get max for each year
    df = df.groupby(['iso_year'], sort=False).agg(
        {'cases': 'max',
         'deaths': 'max',
         'recovered': 'max'
         }).reset_index()

    # convert to int
    df = df.astype(int)

    # overwrite value with actual deaths in 2021
    df.loc[df['iso_year'] == 2021, 'cases'] = df.loc[1]['cases'] - df.loc[0]['cases']
    df.loc[df['iso_year'] == 2021, 'deaths'] = df.loc[1]['deaths'] - df.loc[0]['deaths']
    df.loc[df['iso_year'] == 2021, 'recovered'] = df.loc[1]['recovered'] - df.loc[0]['recovered']

    df['active_cases'] = df['cases'] - (df['deaths'] + df['recovered'])

    # merge calendar_yr foreign key
    df = db.merge_fk(df,
                     table='calendar_yr',
                     df_fk='iso_year',
                     table_fk='iso_year',
                     drop_columns=['iso_year']
                     )

    db.insert_and_append(df, insert_into)

    db.db_close()


def daily_rvalue(df: pd.DataFrame, insert_into: str):
    # create db connection
    db = database.ProjDB()

    # fill NaN with 0
    df = df.fillna(0)

    # convert to datetime type
    df['Datum'] = pd.to_datetime(df['Datum'])

    # create ISO dates
    df = df.assign(iso_key=df['Datum'].dt.strftime('%G%V').astype(int))

    # merge calendar_yr foreign key
    df = db.merge_fk(df,
                     table='calendar_cw',
                     df_fk='iso_key',
                     table_fk='iso_key',
                     drop_columns=['iso_key', 'calendar_yr_id', 'iso_cw']
                     )

    db.insert_and_append(df, insert_into)

    db.db_close()


def weekly_tests(df: pd.DataFrame, insert_into: str):
    # create dbf connection
    db = database.ProjDB()

    # rename columns
    df.columns = ['calendar_week', 'amount_tests', 'positiv_tests', 'positiv_percentage',
                  'amount_transferring_laboratories']

    # delete first & last row
    df = df[1:]
    df = df[:-1]

    # create ISO dates
    df[['iso_cw', 'iso_year']] = df['calendar_week'].str.split('/', expand=True)
    df['iso_cw'] = df['iso_cw'].str.zfill(2)
    df['iso_key'] = df['iso_year'] + df['iso_cw']
    df['iso_key'] = pd.to_numeric(df['iso_key'], errors='coerce')
    df = df \
        .drop('iso_year', axis=1) \
        .drop('iso_cw', axis=1) \
        .drop('calendar_week', axis=1)

    # merge calendar_yr foreign key
    df = db.merge_fk(df,
                     table='calendar_cw',
                     df_fk='iso_key',
                     table_fk='iso_key',
                     drop_columns=['iso_key', 'calendar_yr_id', 'iso_cw']
                     )

    db.insert_and_append(df, insert_into)

    db.db_close()


def weekly_tests_states(df: pd.DataFrame, insert_into: str):
    # create dbf connection
    db = database.ProjDB()

    # delete first & last row
    tmp = df.iloc[4:, :].copy()

    # rename columns
    tmp.columns = ['state', 'iso_year', 'iso_cw', 'amount_tests', 'positiv_percentage']

    # create ISO dates
    tmp['iso_cw'] = tmp['iso_cw'].astype(str)
    tmp['iso_year'] = tmp['iso_year'].astype(str)

    tmp['iso_cw'] = tmp['iso_cw'].str.zfill(2)
    tmp['iso_key'] = tmp['iso_year'] + tmp['iso_cw']
    tmp['iso_key'] = pd.to_numeric(tmp['iso_key'], errors='coerce')

    tmp = tmp \
        .drop('iso_year', axis=1) \
        .drop('iso_cw', axis=1)

    # merge calendar_yr foreign key
    tmp = db.merge_fk(tmp,
                      table='calendar_cw',
                      df_fk='iso_key',
                      table_fk='iso_key',
                      drop_columns=['iso_key', 'calendar_yr_id', 'iso_cw']
                      )

    dct = {'Baden-Württemberg': 8,
           'Bayern': 9,
           'Berlin': 11,
           'Brandenburg': 12,
           'Bremen': 4,
           'Hamburg': 2,
           'Hessen': 6,
           'Mecklenburg-Vorpommern': 13,
           'Niedersachsen': 3,
           'Nordrhein-Westfalen': 5,
           'Rheinland-Pfalz': 7,
           'Saarland': 10,
           'Sachsen': 14,
           'Sachsen-Anhalt': 15,
           'Schleswig-Holstein': 1,
           'Thüringen': 16,
           'unbekannt': 18
           }

    tmp = tmp.assign(ger_states_id=tmp['state'].map(dct))
    del tmp['state']

    db.insert_and_append(tmp, insert_into)

    db.db_close()


def calc_numbers(df: pd.DataFrame, date: dt.datetime):
    if 'NeuerFall' in df.columns:
        df['cases'] = df[['AnzahlFall']] \
            .sum(axis=1) \
            .where(df['NeuerFall'] >= 0, 0)

        df['cases_delta'] = df[['AnzahlFall']] \
            .sum(axis=1) \
            .where(
            (df['NeuerFall'] == 1) | (df['NeuerFall'] == -1),
            0
        )

        df['cases_7d'] = df[['AnzahlFall']] \
            .sum(axis=1) \
            .where(
            (df['NeuerFall'] >= 0) & (df['Meldedatum'] > date - dt.timedelta(days=8)),
            0
        )
    else:
        df['cases'] = df['AnzahlFall']
        df['cases_delta'] = 0

        df['cases_7d'] = df[['AnzahlFall']] \
            .sum(axis=1) \
            .where(df['Meldedatum'] > date - dt.timedelta(days=8), 0)

    if 'IstErkrankungsbeginn' in df.columns:
        df['cases_7d_sympt'] = df[['AnzahlFall']] \
            .sum(axis=1) \
            .where(
            (df['NeuerFall'] >= 0) &
            (df['Meldedatum'] > date - dt.timedelta(days=8)) &
            (df['IstErkrankungsbeginn'] == 1),
            0
        )
    else:
        df['cases_7d_sympt'] = df[['cases_7d']]

    if 'Refdatum' in df.columns:
        df['cases_delta_ref'] = df[['AnzahlFall']] \
            .sum(axis=1) \
            .where(
            ((df['NeuerFall'] == 1) | (df['NeuerFall'] == -1)) &
            (df['Refdatum'] > date - dt.timedelta(days=2)),
            0
        )

        df['cases_7d_ref'] = df[['AnzahlFall']] \
            .sum(axis=1) \
            .where(
            (df['NeuerFall'] >= 0) &
            (df['Meldedatum'] > date - dt.timedelta(days=8)) &
            (df['Refdatum'] > date - dt.timedelta(days=8)),
            0
        )
    else:
        df['cases_delta_ref'] = df[['cases_delta']]
        df['cases_7d_ref'] = df[['cases_7d']]

    if 'Refdatum' in df.columns and 'IstErkrankungsbeginn' in df.columns:
        df['cases_delta_ref_sympt'] = df[['AnzahlFall']] \
            .sum(axis=1) \
            .where(
            ((df['NeuerFall'] == 1) | (df['NeuerFall'] == -1)) &
            (df['Refdatum'] > date - dt.timedelta(days=2)) &
            (df['IstErkrankungsbeginn'] == 1),
            0
        )

        df['cases_7d_ref_sympt'] = df[['AnzahlFall']] \
            .sum(axis=1) \
            .where(
            (df['NeuerFall'] >= 0) &
            (df['Meldedatum'] > date - dt.timedelta(days=8)) &
            (df['Refdatum'] > date - dt.timedelta(days=8)) &
            (df['IstErkrankungsbeginn'] == 1),
            0
        )
    else:
        df['cases_delta_ref_sympt'] = df[['cases_delta']]
        df['cases_7d_ref_sympt'] = df[['cases_7d']]

    if 'NeuerTodesfall' in df.columns:
        df['deaths'] = df[['AnzahlTodesfall']] \
            .sum(axis=1) \
            .where(df['NeuerTodesfall'] >= 0, 0)

        df['deaths_delta'] = df[['AnzahlTodesfall']] \
            .sum(axis=1) \
            .where(
            (df['NeuerTodesfall'] == 1) | (df['NeuerTodesfall'] == -1),
            0
        )
    else:
        df['deaths'] = df['AnzahlTodesfall']
        df['deaths_delta'] = 0

    if 'NeuGenesen' in df.columns:
        df['recovered'] = df[['AnzahlGenesen']] \
            .sum(axis=1) \
            .where(df['NeuGenesen'] >= 0, 0)

        df['recovered_delta'] = df[['AnzahlGenesen']] \
            .sum(axis=1) \
            .where(
            (df['NeuGenesen'] == 1) | (df['NeuGenesen'] == -1),
            0
        )
    else:
        if 'AnzahlGenesen' in df.columns:
            df['recovered'] = df['AnzahlGenesen']
            df['recovered_delta'] = 0
        else:
            df['recovered'] = 0
            df['recovered_delta'] = 0

    # corona active cases
    df['active_cases'] = df['cases'] - (df['deaths'] + df['recovered'])
    df['active_cases_delta'] = df['cases_delta'] - (df['deaths_delta'] + df['recovered_delta'])

    df['reporting_date'] = date

    df.rename(
        columns={'Altersgruppe': 'rki_agegroups'},
        inplace=True
    )

    df = df[
        ['IdBundesland',
         'rki_agegroups',
         'reporting_date',
         'cases',
         'cases_delta',
         'cases_delta_ref',
         'cases_delta_ref_sympt',
         'cases_7d',
         'cases_7d_sympt',
         'cases_7d_ref',
         'cases_7d_ref_sympt',
         'deaths',
         'deaths_delta',
         'recovered',
         'recovered_delta',
         'active_cases',
         'active_cases_delta'
         ]
    ]

    return df


def create_iso_key(df: pd.DataFrame):
    df = df.assign(iso_key=df['reporting_date'].dt.strftime('%G%V').astype(int))
    return df
