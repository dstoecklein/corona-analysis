import pandas as pd
import datetime as dt
from src.database import db_helper as database


# TODO:
# MAKE TRANSFORMS RETURN DF AND PIPE DB INSERT ETC.
# MAKE FUNCTION FOR INCIDENCE CALC

def covid_daily(df: pd.DataFrame, date: dt.datetime, table: str):
    # create db connection
    db = database.ProjDB()

    # get ger population
    population = db.get_population(country='DE', country_code='iso_3166_1_alpha2', year='2020')

    # calculate rki corona numbers
    tmp = calc_numbers(df=df, date=date)
    tmp = tmp.groupby('reporting_date').sum().reset_index()

    # incidence 7 days
    tmp['incidence_7d'] = (tmp['cases_7d'] / population) * 100000
    tmp['incidence_7d_sympt'] = (tmp['cases_7d_sympt'] / population) * 100000
    tmp['incidence_7d_ref'] = (tmp['cases_7d_ref'] / population) * 100000
    tmp['incidence_7d_ref_sympt'] = (tmp['cases_7d_ref_sympt'] / population) * 100000

    # merge foreign key
    tmp = db.merge_calendar_days_fk(df=tmp, left_on='reporting_date')

    tmp['geo'] = 'DE'
    tmp = db.merge_countries_fk(df=tmp, left_on='geo', country_code='iso_3166_1_alpha2')

    # insert only new rows, update old
    db.insert_or_update(df=tmp, table=table)

    db.db_close()


def covid_weekly_cummulative(df: pd.DataFrame, date: dt.datetime, table: str):
    # create db connection
    db = database.ProjDB()

    tmp = calc_numbers(df=df, date=date)

    # create iso key
    tmp = create_iso_key(df=tmp)

    # merge calendar_yr foreign key
    tmp = db.merge_calendar_weeks_fk(df=tmp, left_on='iso_key')

    tmp = tmp.groupby('calendar_weeks_fk').sum().reset_index()

    tmp['geo'] = 'DE'
    tmp = db.merge_countries_fk(df=tmp, left_on='geo', country_code='iso_3166_1_alpha2')

    # insert only new rows, update old
    db.insert_or_update(df=tmp, table=table)

    db.db_close()


def covid_daily_states(df: pd.DataFrame, date: dt.datetime, table: str):
    db = database.ProjDB()

    tmp = calc_numbers(df, date)

    df_population_by_states = db.get_population_by_states(country='DE', country_code='iso_3166_1_alpha2', year='2020',
                                                          level=1)
    df_subdivision_ids = df_population_by_states[['country_subdivs_1_id', 'bundesland_id', 'population']].copy()

    # merge states population
    tmp = tmp.merge(df_subdivision_ids,
                    left_on='IdBundesland',
                    right_on='bundesland_id',
                    how='left',
                    ).drop(['country_subdivs_1_id'], axis=1)

    # incidence 7 days
    tmp['incidence_7d'] = (tmp['cases_7d'] / tmp['population']) * 100000
    tmp['incidence_7d_sympt'] = (tmp['cases_7d_sympt'] / tmp['population']) * 100000
    tmp['incidence_7d_ref'] = (tmp['cases_7d_ref'] / tmp['population']) * 100000
    tmp['incidence_7d_ref_sympt'] = (tmp['cases_7d_ref_sympt'] / tmp['population']) * 100000

    tmp = tmp.groupby(['IdBundesland', 'reporting_date']) \
        .sum() \
        .reset_index()

    # create iso key
    tmp = create_iso_key(tmp)

    # merge foreign key
    tmp = db.merge_calendar_days_fk(df=tmp, left_on='reporting_date')

    # merge subdivision 1 id
    tmp = tmp.merge(df_subdivision_ids,
                    left_on='IdBundesland',
                    right_on='bundesland_id',
                    how='left',
                    )

    tmp.rename(
        columns={'country_subdivs_1_id': 'country_subdivs_1_fk'},
        inplace=True
    )
    tmp = tmp[tmp['IdBundesland'] > 0]  # ignore rows with IdBundesland -1 (nicht erhoben)

    db.insert_or_update(tmp, table)

    db.db_close()


def covid_daily_counties(df: pd.DataFrame, date: dt.datetime, table: str):
    db = database.ProjDB()

    tmp = calc_numbers(df, date)
    tmp = tmp[tmp['IdBundesland'] > 0]  # ignore -nicht erhoben-

    # combine berlin districts
    tmp['IdLandkreis'] = tmp['IdLandkreis'].astype(int).replace({
        11001: 11000,
        11002: 11000,
        11003: 11000,
        11004: 11000,
        11005: 11000,
        11006: 11000,
        11007: 11000,
        11008: 11000,
        11009: 11000,
        11010: 11000,
        11011: 11000,
        11012: 11000
    })

    tmp = tmp.groupby(['IdLandkreis', 'reporting_date']) \
        .sum() \
        .reset_index()

    # create iso key
    tmp = create_iso_key(tmp)

    # merge foreign keys
    tmp = db.merge_calendar_days_fk(df=tmp, left_on='reporting_date')
    tmp = tmp.drop(['iso_key', 'calendar_weeks_fk'], axis=1)

    tmp = db.merge_subdivisions_fk(df=tmp, left_on='IdLandkreis', level=3, subdiv_code='ags')
    tmp = tmp.drop(['country_subdivs_2_fk', 'subdivision_3', 'nuts_3'], axis=1)

    tmp = tmp[tmp['country_subdivs_3_fk'].notna()]  # old csv's had different countie descriptions, so just ignore them

    df_population_by_states = db.get_population_by_states(country='DE', country_code='iso_3166_1_alpha2', year='2020',
                                                          level=3)

    # merge states population
    tmp = tmp.merge(df_population_by_states,
                    left_on='country_subdivs_3_fk',
                    right_on='country_subdivs_3_fk',
                    how='left',
                    ).drop(['population_subdivs_3_id', 'calendar_years_fk', 'last_update', 'unique_key'], axis=1)
    tmp = tmp.drop(['country_subdivs_3_id', 'country_subdivs_2_fk', 'subdivision_3', 'latitude', 'longitude', 'nuts_3', 'ags', 'country_subdivs_2_id', 'country_subdivs_1_fk'], axis=1)

    # incidence 7 days
    tmp['incidence_7d'] = (tmp['cases_7d'] / tmp['population']) * 100000
    tmp['incidence_7d_sympt'] = (tmp['cases_7d_sympt'] / tmp['population']) * 100000
    tmp['incidence_7d_ref'] = (tmp['cases_7d_ref'] / tmp['population']) * 100000
    tmp['incidence_7d_ref_sympt'] = (tmp['cases_7d_ref_sympt'] / tmp['population']) * 100000

    tmp.rename(
        columns={'country_subdivs_3_id': 'country_subdivs_3_fk'},
        inplace=True
    )

    db.insert_or_update(tmp, table)

    db.db_close()


# TODO:
def covid_daily_agegroups(df: pd.DataFrame, date: dt.datetime, insert_into: str):
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


# TODO:
def covid_annual(insert_into: str):
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


# TODO:
def rvalue_daily(df: pd.DataFrame, insert_into: str):
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


def tests_weekly(df: pd.DataFrame, table: str):
    # create dbf connection
    db = database.ProjDB()

    tmp = df.copy()

    # rename columns
    tmp.columns = ['calendar_week', 'amount', 'positive', 'positive_percentage',
                   'amount_transferring_laboratories']

    # delete first & last row
    tmp = tmp[1:]
    tmp = tmp[:-1]

    # create ISO dates
    tmp[['iso_cw', 'iso_year']] = tmp['calendar_week'].str.split('/', expand=True)
    tmp['iso_cw'] = tmp['iso_cw'].str.zfill(2)
    tmp['iso_key'] = tmp['iso_year'] + tmp['iso_cw']
    tmp['iso_key'] = pd.to_numeric(tmp['iso_key'], errors='coerce')

    tmp = tmp \
        .drop('iso_year', axis=1) \
        .drop('iso_cw', axis=1) \
        .drop('calendar_week', axis=1)

    # merge calendar_yr foreign key
    tmp = db.merge_calendar_weeks_fk(df=tmp, left_on='iso_key')

    tmp['geo'] = 'DE'  # just add an extra column to make merge happen
    tmp = db.merge_countries_fk(df=tmp, left_on='geo', country_code='iso_3166_1_alpha2')

    # insert only new rows, update old
    db.insert_or_update(df=tmp, table=table)

    db.db_close()


# TODO:
def tests_weekly_states(df: pd.DataFrame, insert_into: str):
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
    tmp = df.copy()

    if 'NeuerFall' in tmp.columns:
        tmp['cases'] = tmp[['AnzahlFall']] \
            .sum(axis=1) \
            .where(tmp['NeuerFall'] >= 0, 0)

        tmp['cases_delta'] = tmp[['AnzahlFall']] \
            .sum(axis=1) \
            .where(
            (tmp['NeuerFall'] == 1) | (tmp['NeuerFall'] == -1),
            0
        )

        tmp['cases_7d'] = tmp[['AnzahlFall']] \
            .sum(axis=1) \
            .where(
            (tmp['NeuerFall'] >= 0) & (tmp['Meldedatum'] > date - dt.timedelta(days=8)),
            0
        )
    else:
        tmp['cases'] = tmp['AnzahlFall']
        tmp['cases_delta'] = 0

        tmp['cases_7d'] = tmp[['AnzahlFall']] \
            .sum(axis=1) \
            .where(tmp['Meldedatum'] > date - dt.timedelta(days=8), 0)

    if 'IstErkrankungsbeginn' in tmp.columns:
        tmp['cases_7d_sympt'] = tmp[['AnzahlFall']] \
            .sum(axis=1) \
            .where(
            (tmp['NeuerFall'] >= 0) &
            (tmp['Meldedatum'] > date - dt.timedelta(days=8)) &
            (tmp['IstErkrankungsbeginn'] == 1),
            0
        )
    else:
        tmp['cases_7d_sympt'] = tmp[['cases_7d']]

    if 'Refdatum' in tmp.columns:
        tmp['cases_delta_ref'] = tmp[['AnzahlFall']] \
            .sum(axis=1) \
            .where(
            ((tmp['NeuerFall'] == 1) | (tmp['NeuerFall'] == -1)) &
            (tmp['Refdatum'] > date - dt.timedelta(days=2)),
            0
        )

        tmp['cases_7d_ref'] = tmp[['AnzahlFall']] \
            .sum(axis=1) \
            .where(
            (tmp['NeuerFall'] >= 0) &
            (tmp['Meldedatum'] > date - dt.timedelta(days=8)) &
            (tmp['Refdatum'] > date - dt.timedelta(days=8)),
            0
        )
    else:
        tmp['cases_delta_ref'] = tmp[['cases_delta']]
        tmp['cases_7d_ref'] = tmp[['cases_7d']]

    if 'Refdatum' in tmp.columns and 'IstErkrankungsbeginn' in tmp.columns:
        tmp['cases_delta_ref_sympt'] = tmp[['AnzahlFall']] \
            .sum(axis=1) \
            .where(
            ((tmp['NeuerFall'] == 1) | (tmp['NeuerFall'] == -1)) &
            (tmp['Refdatum'] > date - dt.timedelta(days=2)) &
            (tmp['IstErkrankungsbeginn'] == 1),
            0
        )

        tmp['cases_7d_ref_sympt'] = tmp[['AnzahlFall']] \
            .sum(axis=1) \
            .where(
            (tmp['NeuerFall'] >= 0) &
            (tmp['Meldedatum'] > date - dt.timedelta(days=8)) &
            (tmp['Refdatum'] > date - dt.timedelta(days=8)) &
            (tmp['IstErkrankungsbeginn'] == 1),
            0
        )
    else:
        tmp['cases_delta_ref_sympt'] = tmp[['cases_delta']]
        tmp['cases_7d_ref_sympt'] = tmp[['cases_7d']]

    if 'NeuerTodesfall' in tmp.columns:
        tmp['deaths'] = tmp[['AnzahlTodesfall']] \
            .sum(axis=1) \
            .where(tmp['NeuerTodesfall'] >= 0, 0)

        tmp['deaths_delta'] = tmp[['AnzahlTodesfall']] \
            .sum(axis=1) \
            .where(
            (tmp['NeuerTodesfall'] == 1) | (tmp['NeuerTodesfall'] == -1),
            0
        )
    else:
        tmp['deaths'] = tmp['AnzahlTodesfall']
        tmp['deaths_delta'] = 0

    if 'NeuGenesen' in tmp.columns:
        tmp['recovered'] = tmp[['AnzahlGenesen']] \
            .sum(axis=1) \
            .where(tmp['NeuGenesen'] >= 0, 0)

        tmp['recovered_delta'] = tmp[['AnzahlGenesen']] \
            .sum(axis=1) \
            .where(
            (tmp['NeuGenesen'] == 1) | (tmp['NeuGenesen'] == -1),
            0
        )
    else:
        if 'AnzahlGenesen' in tmp.columns:
            tmp['recovered'] = tmp['AnzahlGenesen']
            tmp['recovered_delta'] = 0
        else:
            tmp['recovered'] = 0
            tmp['recovered_delta'] = 0

    # corona active cases
    tmp['active_cases'] = tmp['cases'] - (tmp['deaths'] + tmp['recovered'])
    tmp['active_cases_delta'] = tmp['cases_delta'] - (tmp['deaths_delta'] + tmp['recovered_delta'])

    tmp['reporting_date'] = date

    tmp.rename(
        columns={
            'Altersgruppe': 'rki_agegroups'
        },
        inplace=True
    )

    tmp = tmp[
        ['IdBundesland',
         'IdLandkreis',
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

    return tmp


def create_iso_key(df: pd.DataFrame):
    tmp = df.copy()
    tmp = tmp.assign(iso_key=tmp['reporting_date'].dt.strftime('%G%V').astype(int))
    return tmp
