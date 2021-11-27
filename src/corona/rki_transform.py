import pandas as pd
from src.database import db_helper as database
from src.utils import date_helper, rki_helper


def covid_daily(df: pd.DataFrame):
    db = database.ProjDB()
    tmp = df.copy()
    tmp = tmp.groupby('reporting_date').sum().reset_index()
    tmp['geo'] = 'DE'
    tmp = rki_helper.calc_7d_incidence(df=tmp, level=0, reference_year='2020')
    tmp = db.merge_calendar_days_fk(df=tmp, left_on='reporting_date')
    db.db_close()
    return tmp


def covid_daily_states(df: pd.DataFrame):
    db = database.ProjDB()
    tmp = df.copy()
    tmp = tmp[tmp['IdBundesland'] > 0]  # ignore rows with IdBundesland -1 (nicht erhoben)
    tmp = tmp.groupby(['IdBundesland', 'reporting_date']).sum().reset_index()
    tmp = rki_helper.calc_7d_incidence(df=tmp, level=1, reference_year='2020')
    tmp = db.merge_calendar_days_fk(df=tmp, left_on='reporting_date')
    db.db_close()
    return tmp


def covid_daily_counties(df: pd.DataFrame):
    db = database.ProjDB()
    tmp = df.copy()
    tmp = tmp[tmp['IdBundesland'] > 0]  # ignore rows with IdBundesland -1 (nicht erhoben)

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

    tmp = tmp.groupby(['IdLandkreis', 'reporting_date']).sum().reset_index()
    tmp = rki_helper.calc_7d_incidence(df=tmp, level=3, reference_year='2021')
    tmp = db.merge_calendar_days_fk(df=tmp, left_on='reporting_date')
    db.db_close()
    return tmp


def covid_daily_agegroups(df: pd.DataFrame):
    db = database.ProjDB()
    tmp = df.copy()
    tmp = tmp.groupby(['rki_agegroups', 'reporting_date']).sum().reset_index()

    tmp.replace(
        {
            'rki_agegroups':
                {
                    'A00-A04': '00-04',
                    'A05-A14': '05-14',
                    'A15-A34': '15-34',
                    'A35-A59': '35-59',
                    'A60-A79': '60-79',
                    'A80+': '80+',
                    'unbekannt': 'UNK'
                }
        }, inplace=True
    )

    tmp['geo'] = 'DE'
    tmp = rki_helper.calc_7d_incidence(df=tmp, level=0, reference_year='2020')
    tmp = db.merge_calendar_days_fk(df=tmp, left_on='reporting_date')
    tmp = db.merge_agegroups_fk(df=tmp, left_on='rki_agegroups', interval='rki')
    db.db_close()
    return tmp


def covid_weekly_cummulative(df: pd.DataFrame):
    db = database.ProjDB()
    tmp = df.copy()
    tmp = date_helper.create_iso_key(df=tmp, column_name='reporting_date')
    tmp = tmp.groupby('iso_key').sum().reset_index()
    tmp = db.merge_calendar_weeks_fk(df=tmp, left_on='iso_key')
    tmp['geo'] = 'DE'
    tmp = db.merge_countries_fk(df=tmp, left_on='geo', country_code='nuts_0')
    db.db_close()
    return tmp


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


#TODO: wie oben anpassen
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
