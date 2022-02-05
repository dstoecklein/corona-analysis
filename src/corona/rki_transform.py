import datetime as dt
import pandas as pd
from src.utils import covid_helper, date_helper, rki_helper, db_helper as database


# def covid_daily(df: pd.DataFrame, date: dt.datetime):
#     db = database.ProjDB()
#     tmp = df.copy()
#     tmp = rki_helper.pre_process_covid(df=tmp)
#     tmp = rki_helper.covid_calc_numbers(df=tmp, date=date)
#     tmp = tmp.groupby('reporting_date').sum().reset_index()
#     tmp['geo'] = 'DE'
#     tmp = rki_helper.calc_7d_incidence(df=tmp, level=0, reference_year='2020')
#     tmp = db.merge_calendar_days_fk(df=tmp, left_on='reporting_date')
#     db.db_close()
#     return tmp


def covid_daily_states(df: pd.DataFrame, date: dt.datetime):
    db = database.ProjDB()
    tmp = df.copy()
    tmp = rki_helper.pre_process_covid(tmp)
    tmp = rki_helper.covid_calc_numbers(df=tmp, date=date)
    tmp = tmp[tmp['IdBundesland'] > 0]  # ignore rows with IdBundesland -1 (nicht erhoben)
    tmp = tmp.groupby(['IdBundesland', 'reporting_date']).sum().reset_index()
    tmp = rki_helper.calc_7d_incidence(df=tmp, level=1, reference_year='2020')
    tmp = db.merge_calendar_days_fk(df=tmp, left_on='reporting_date')
    db.db_close()
    return tmp


def covid_daily_counties(df: pd.DataFrame, date: dt.datetime):
    db = database.ProjDB()
    tmp = df.copy()
    tmp = rki_helper.pre_process_covid(tmp)
    tmp = rki_helper.covid_calc_numbers(df=tmp, date=date)
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


def covid_daily_agegroups(df: pd.DataFrame, date: dt.datetime):
    db = database.ProjDB()
    tmp = df.copy()
    tmp = rki_helper.pre_process_covid(tmp)
    tmp = tmp[tmp['Geschlecht'] != 'unbekannt']
    tmp = rki_helper.covid_calc_numbers(df=tmp, date=date)
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
    tmp = rki_helper.pre_process_covid(df=tmp)
    tmp = rki_helper.covid_calc_numbers(df=tmp, date=tmp['Meldedatum'])
    tmp = date_helper.create_iso_key(df=tmp, column_name='reporting_date')
    tmp = tmp.groupby('iso_key').sum().reset_index()
    tmp = db.merge_calendar_weeks_fk(df=tmp, left_on='iso_key')
    tmp['geo'] = 'DE'
    tmp = db.merge_countries_fk(df=tmp, left_on='geo', country_code='nuts_0')
    db.db_close()
    return tmp


def tests_weekly(df: pd.DataFrame):
    db = database.ProjDB()
    tmp = df.copy()
    tmp = rki_helper.pre_process_tests(df=tmp)
    tmp = db.merge_calendar_weeks_fk(df=tmp, left_on='iso_key')
    tmp['geo'] = 'DE'
    tmp = db.merge_countries_fk(df=tmp, left_on='geo', country_code='iso_3166_1_alpha2')
    db.db_close()
    return tmp


def rvalue_daily(df: pd.DataFrame):
    db = database.ProjDB()
    tmp = df.copy()
    tmp = rki_helper.pre_process_rvalue(df=tmp)
    tmp = db.merge_calendar_days_fk(df=tmp, left_on='date')
    tmp['geo'] = 'DE'
    tmp = db.merge_countries_fk(df=tmp, left_on='geo', country_code='iso_3166_1_alpha2')
    db.db_close()
    return tmp


def vaccinations_daily_states(df: pd.DataFrame):
    db = database.ProjDB()
    tmp = df.copy()
    tmp = rki_helper.pre_process_vaccination_states(df=tmp)
    tmp = db.merge_vaccines_fk(df=tmp, left_on='Impfstoff')
    tmp = db.merge_calendar_days_fk(df=tmp, left_on='Impfdatum')
    tmp = db.merge_subdivisions_fk(df=tmp, left_on='BundeslandId_Impfort', level=1, subdiv_code='bundesland_id')
    tmp = db.merge_vaccine_series_fk(df=tmp, left_on='Impfserie')
    db.db_close()
    return tmp