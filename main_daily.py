import pandas as pd
import datetime as dt
from src.utils import paths
from src.corona import rki_transform
from src.database import db_helper as database
from src.hospitals import divi_transform, genesis_transform
from src.web_scraper import rki_scrap, divi_scrap, genesis_scrap
from src.utils import rki_helper
import os
import re

"""
Runs daily via batch
"""

COVID_PATH = paths.get_covid19_ger_path()
HOSP_PATH = paths.get_hospitals_ger_path()


def main():
    db = database.ProjDB()

    df_rki = rki_scrap.covid_daily(save_file=True)
    df_divi = divi_scrap.itcu_daily_counties(save_file=True)

    today = dt.date.today()
    today = dt.datetime(today.year, today.month, today.day)

    # --Pre-Processing--
    try:
        if 'Meldedatum' in df_rki.columns:
            df_rki['Meldedatum'] = pd.to_datetime(df_rki['Meldedatum'], infer_datetime_format=True).dt.date
            df_rki['Meldedatum'] = pd.to_datetime(df_rki['Meldedatum'], infer_datetime_format=True)
        if 'Refdatum' in df_rki.columns:
            df_rki['Refdatum'] = pd.to_datetime(df_rki['Refdatum'], infer_datetime_format=True).dt.date
            df_rki['Refdatum'] = pd.to_datetime(df_rki['Refdatum'], infer_datetime_format=True)
    except (KeyError, TypeError):
        print('Error trying to convert Date columns')

    # remove whitespaces from header
    df_rki.columns = df_rki.columns.str.replace(' ', '')

    # --Calculation--
    df = rki_helper.calc_numbers(df=df_rki, date=today)
    # specific for agegroups - ignore unknown agegroups, otherwise wrong calculation
    df_rki_daily_agegroups = df_rki[df_rki['Geschlecht'] != 'unbekannt']
    df_rki_daily_agegroups = rki_helper.calc_numbers(df=df_rki_daily_agegroups, date=today)
    # specific for weekly numbers
    df_rki_weekly_cumulative = rki_helper.calc_numbers(df=df_rki, date=df_rki['Meldedatum'])

    # --Transformation--
    df_rki_daily = rki_transform.covid_daily(df=df)
    df_rki_daily_states = rki_transform.covid_daily_states(df=df)
    df_rki_daily_counties = rki_transform.covid_daily_counties(df=df)
    df_rki_daily_agegroups = rki_transform.covid_daily_agegroups(df=df_rki_daily_agegroups)
    df_rki_weekly_cumulative = rki_transform.covid_weekly_cummulative(df=df_rki_weekly_cumulative)

    # --DB insert--
    db.insert_or_update(df=df_rki_daily, table='covid_daily')
    db.insert_or_update(df=df_rki_daily_states, table='covid_daily_states')
    db.insert_or_update(df=df_rki_daily_counties, table='covid_daily_counties')
    db.insert_or_update(df=df_rki_daily_agegroups, table='covid_daily_agegroups')
    db.insert_or_update(df=df_rki_weekly_cumulative, table='covid_weekly_cumulative')

    db.db_close()


def rki_bulk_procedure():
    db = database.ProjDB()

    for filename in os.listdir(COVID_PATH):
        if filename.endswith('.csv') or filename.endswith('.xz'):
            extract = re.search(r'\d{4}-\d{2}-\d{2}', filename)
            date = dt.datetime.strptime(extract.group(), '%Y-%m-%d')

            try:
                df_rki = pd.read_csv(COVID_PATH + filename, engine='python', sep=',', encoding='utf8')
            except UnicodeDecodeError:
                df_rki = pd.read_csv(COVID_PATH + filename, engine='python', sep=',', encoding='ISO-8859-1')

            # first convert to date then to datetime, because of different date values in older .csv files
            try:
                if 'Meldedatum' in df_rki.columns:  # rki
                    df_rki['Meldedatum'] = pd.to_datetime(df_rki['Meldedatum'], infer_datetime_format=True).dt.date
                    df_rki['Meldedatum'] = pd.to_datetime(df_rki['Meldedatum'], infer_datetime_format=True)
                if 'Refdatum' in df_rki.columns:  # rki
                    df_rki['Refdatum'] = pd.to_datetime(df_rki['Refdatum'], infer_datetime_format=True).dt.date
                    df_rki['Refdatum'] = pd.to_datetime(df_rki['Refdatum'], infer_datetime_format=True)
            except (KeyError, TypeError):
                print('Error trying to convert Date columns')

            # remove whitespaces from header
            df_rki.columns = df_rki.columns.str.replace(' ', '')

            # --Calculation--
            df = rki_helper.calc_numbers(df=df_rki, date=date)
            # specific for agegroups - ignore unknown agegroups, otherwise wrong calculation
            df_rki_daily_agegroups = df_rki[df_rki['Geschlecht'] != 'unbekannt']
            df_rki_daily_agegroups = rki_helper.calc_numbers(df=df_rki_daily_agegroups, date=date)
            # specific for weekly numbers
            df_rki_weekly_cumulative = rki_helper.calc_numbers(df=df_rki, date=df_rki['Meldedatum'])

            # --Transformation--
            df_rki_daily = rki_transform.covid_daily(df=df)
            df_rki_daily_states = rki_transform.covid_daily_states(df=df)
            df_rki_daily_counties = rki_transform.covid_daily_counties(df=df)
            df_rki_daily_agegroups = rki_transform.covid_daily_agegroups(df=df_rki_daily_agegroups)
            df_rki_weekly_cumulative = rki_transform.covid_weekly_cummulative(df=df_rki_weekly_cumulative)

            # --DB insert--
            db.insert_or_update(df=df_rki_daily, table='covid_daily')
            db.insert_or_update(df=df_rki_daily_states, table='covid_daily_states')
            db.insert_or_update(df=df_rki_daily_counties, table='covid_daily_counties')
            db.insert_or_update(df=df_rki_daily_agegroups, table='covid_daily_agegroups')
            db.insert_or_update(df=df_rki_weekly_cumulative, table='covid_weekly_cumulative')

            db.db_close()


def divi_bulk_procedure():
    for filename in os.listdir(HOSP_PATH):
        if filename.endswith('.csv') or filename.endswith('.xz'):

            try:
                df = pd.read_csv(HOSP_PATH + filename, engine='python', sep=',', encoding='utf8')
            except UnicodeDecodeError:
                df = pd.read_csv(HOSP_PATH + filename, engine='python', sep=',', encoding='ISO-8859-1')

            if '_COUNTIES' in filename:

                try:
                    if 'date' in df.columns:
                        df['date'] = pd.to_datetime(df['date'], infer_datetime_format=True).dt.date
                        df['date'] = pd.to_datetime(df['date'], infer_datetime_format=True)
                except (KeyError, TypeError):
                    print('Error trying to convert Date columns')

                divi_transform.itcu_daily_counties(df=df, table='itcu_daily_counties')

            elif '_STATES' in filename:

                try:
                    if 'date' in df.columns:
                        df['Datum'] = pd.to_datetime(df['Datum'], infer_datetime_format=True, utc=True).dt.date
                        df['Datum'] = pd.to_datetime(df['Datum'], infer_datetime_format=True)
                except (KeyError, TypeError):
                    print('Error trying to convert Date columns')

                divi_transform.itcu_daily_states(df=df, table='itcu_daily_states')
                # divi_transform.daily_itcu_states(df=df, table='itcu_daily_states')


if __name__ == '__main__':
    main()
