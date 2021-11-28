import pandas as pd
from src.database import db_helper as database
from src.utils import divi_helper


def itcu_daily_counties(df: pd.DataFrame):
    db = database.ProjDB()
    tmp = df.copy()
    tmp = divi_helper.pre_process(tmp)
    tmp = db.merge_subdivisions_fk(df=tmp, left_on='gemeindeschluessel', level=3, subdiv_code='ags')
    tmp = db.merge_calendar_days_fk(df=tmp, left_on='date')
    tmp.rename(
        columns={
            'anzahl_standorte': 'amount_hospital_locations',
            'anzahl_meldebereiche': 'amount_reporting_areas',
            'faelle_covid_aktuell': 'cases_covid',
            'faelle_covid_aktuell_invasiv_beatmet': 'cases_covid_invasive_ventilated',
            'betten_frei': 'itcu_free',
            'betten_frei_nur_erwachsen': 'itcu_free_adults',
            'betten_belegt': 'itcu_occupied',
            'betten_belegt_nur_erwachsen': 'itcu_occupied_adults',

        },
        inplace=True
    )
    db.db_close()
    return tmp


def itcu_daily_states(df: pd.DataFrame):
    db = database.ProjDB()
    tmp = df.copy()
    tmp = divi_helper.pre_process(tmp)
    tmp['Bundesland'] = tmp['Bundesland'].str.lower()
    tmp['Bundesland'] = tmp['Bundesland'].str.replace('ae', 'ä')
    tmp['Bundesland'] = tmp['Bundesland'].str.replace('ue', 'ü')
    tmp['Bundesland'] = tmp['Bundesland'].str.replace('oe', 'ö')
    tmp['Bundesland'] = tmp['Bundesland'].str.replace('_', '-')
    tmp['Bundesland'] = tmp['Bundesland'].astype(str)
    tmp = db.merge_subdivisions_fk(df=tmp, left_on='Bundesland', level=1, subdiv_code='subdivision_1')
    tmp = db.merge_calendar_days_fk(df=tmp, left_on='Datum')
    tmp['faelle_covid_erstaufnahmen'] = tmp['faelle_covid_erstaufnahmen'].fillna(0)
    tmp.rename(
        columns={
            'Behandlungsgruppe': 'treatment_group',
            'Anzahl_Meldebereiche': 'amount_reporting_areas',
            'Aktuelle_COVID_Faelle_ITS': 'cases_covid',
            'Belegte_Intensivbetten': 'itcu_occupied',
            'Freie_Intensivbetten': 'itcu_free',
            '7_Tage_Notfallreserve': '7_day_emergency_reserve',
            'Freie_IV_Kapazitaeten_Gesamt': 'free_capacities_invasive_treatment',
            'Freie_IV_Kapazitaeten_Davon_COVID': 'free_capacities_invasive_treatment_covid',
            'Betriebssituation_Regulaerer_Betrieb': 'operating_situation_regular',
            'Betriebssituation_Teilweise_Eingeschraenkt': 'operating_situation_partially_restricted',
            'Betriebssituation_Eingeschraenkt': 'operating_situation_restricted',
            'Betriebssituation_Keine_Angabe': 'operating_situation_not_specified',
            'faelle_covid_erstaufnahmen': 'cases_covid_initial_reception'
        },
        inplace=True
    )
    db.db_close()
    return tmp
