import pandas as pd
import datetime as dt
from src.utils import paths
from src.corona import rki_transform
from src.database import db_helper as database
from src.hospitals import divi_transform
from src.web_scraper import rki_scrap, divi_scrap
from src.utils import rki_helper, divi_helper
import os
import re

"""
Runs daily via batch
"""

COVID_PATH = paths.get_covid19_ger_path()
HOSP_PATH = paths.get_hospitals_ger_path()


def main():
    rki_procedure()
    divi_procedure()
    # rki_bulk_procedure()
    # divi_bulk_procedure()


def divi_procedure():
    db = database.ProjDB()

    # --Scraping Data--
    df_divi_counties = divi_scrap.itcu_daily_counties(save_file=True)
    df_divi_states = divi_scrap.itcu_daily_states(save_file=True)

    # --Pre-Processing--
    df_divi_counties = divi_helper.pre_process(df_divi_counties)
    df_divi_states = divi_helper.pre_process(df_divi_states)

    # --Transformation--
    df_divi_counties = divi_transform.itcu_daily_counties(df=df_divi_counties)
    df_divi_states = divi_transform.itcu_daily_states(df=df_divi_states)

    # --DB insert--
    db.insert_or_update(df=df_divi_counties, table='itcu_daily_counties')
    db.insert_or_update(df=df_divi_states, table='itcu_daily_states')

    db.db_close()


def rki_procedure():
    db = database.ProjDB()

    # --Scraping Data--
    df_rki = rki_scrap.covid_daily(save_file=True)

    today = dt.date.today()
    today = dt.datetime(today.year, today.month, today.day)

    # --Pre-Processing--
    df_rki = rki_helper.pre_process(df_rki)

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

            # --Pre-Processing--
            df_rki = rki_helper.pre_process(df_rki)

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
    db = database.ProjDB()

    for filename in os.listdir(HOSP_PATH):
        if filename.endswith('.csv') or filename.endswith('.xz'):

            try:
                df = pd.read_csv(HOSP_PATH + filename, engine='python', sep=',', encoding='utf8')
            except UnicodeDecodeError:
                df = pd.read_csv(HOSP_PATH + filename, engine='python', sep=',', encoding='ISO-8859-1')

            # --Pre-Processing--
            df = divi_helper.pre_process(df)

            if '_COUNTIES' in filename:
                df_divi_counties = divi_transform.itcu_daily_counties(df=df)
                db.insert_or_update(df=df_divi_counties, table='itcu_daily_counties')
            elif '_STATES' in filename:
                df_divi_states = divi_transform.itcu_daily_states(df=df)
                db.insert_or_update(df=df_divi_states, table='itcu_daily_states')
            else:
                return

            db.db_close()


if __name__ == '__main__':
    main()
