import pandas as pd
from src.database import db_helper as database


def itcu_daily_counties(df: pd.DataFrame, table: str):
    db = database.ProjDB()

    tmp = df.copy()

    tmp = db.merge_subdivisions_fk(df=tmp, left_on='gemeindeschluessel', level=3, subdiv_code='ags')

    tmp = db.merge_calendar_days_fk(df=tmp, left_on='date')

    cols = [
        'amount_hospital_locations',
        'amount_reporting_areas',
        'cases_covid',
        'cases_covid_invasive_ventilated',
        'itcu_free',
        'itcu_free_adults',
        'itcu_occupied',
        'itcu_occupied_adults'
    ]
    tmp.columns = cols

    db.insert_or_update(df=tmp, table=table)

    db.db_close()


def itcu_daily_states(df: pd.DataFrame, table: str):
    db = database.ProjDB()

    tmp = df.copy()

    tmp['Bundesland'] = tmp['Bundesland'].str.lower()
    tmp['Bundesland'] = tmp['Bundesland'].str.replace('ae', 'ä')
    tmp['Bundesland'] = tmp['Bundesland'].str.replace('ue', 'ü')
    tmp['Bundesland'] = tmp['Bundesland'].str.replace('oe', 'ö')
    tmp['Bundesland'] = tmp['Bundesland'].str.replace('_', '-')
    tmp['Bundesland'] = tmp['Bundesland'].astype(str)

    tmp = db.merge_subdivisions_fk(df=tmp, left_on='Bundesland', level=1, subdiv_code='subdivision_1')

    tmp['Datum'] = pd.to_datetime(tmp['Datum'], infer_datetime_format=True, utc=True).dt.date
    tmp['Datum'] = pd.to_datetime(tmp['Datum'], infer_datetime_format=True)

    tmp = db.merge_calendar_days_fk(df=tmp, left_on='Datum')

    tmp['faelle_covid_erstaufnahmen'] = tmp['faelle_covid_erstaufnahmen'].fillna(0)

    cols = [
        'treatment_group',
        'amount_reporting_areas',
        'cases_covid',
        'itcu_occupied',
        'itcu_free',
        '7_day_emergency_reserve',
        'free_capacities_invasive_treatment',
        'free_capacities_invasive_treatment_covid',
        'operating_situation_regular',
        'operating_situation_partially_restricted',
        'operating_situation_restricted',
        'operating_situation_not_specified',
        'cases_covid_initial_reception'
    ]
    tmp.columns = cols

    db.insert_or_update(df=tmp, table=table)

    db.db_close()
