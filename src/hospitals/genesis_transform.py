import pandas as pd
from src.database import db_helper as database


def annual_hospitals(df: pd.DataFrame, table: str):
    # create db connection
    db = database.ProjDB()

    tmp = df.copy()

    cols = [
        'iso_year',
        'amount_hospitals',
        'amount_beds',
        'amount_beds_per_100000_population',
        'amount_patients',
        'amount_patients_per_100000_population',
        'occupancy_days',
        'avg_days_of_hospitalization',
        'avg_bed_occupancy_percent'
    ]
    tmp.columns = cols

    tmp = db.merge_calendar_years_fk(df=tmp, left_on='iso_year')

    tmp['geo'] = 'DE'
    tmp = db.merge_countries_fk(df=tmp, left_on='geo', country_code='iso_3166_1_alpha2')

    # insert only new rows, update old
    db.insert_or_update(df=tmp, table=table)

    db.db_close()
