import pandas as pd
from src.database import db_helper as database


def daily_itcu_cumulative(df: pd.DataFrame, table: str):
    db = database.ProjDB()

    tmp = df.copy()

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

    db.insert_or_update(df=tmp, table=table)

    db.db_close()
