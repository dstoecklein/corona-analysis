import pandas as pd
import datetime as dt
from src.utils import paths, db_helper as database
from src.corona import rki_transform
from src.hospitals import divi_transform
from src.web_scraper import rki_scrap, divi_scrap
import os
import re

"""
Runs daily via batch
"""

COVID_PATH = paths.get_covid_path()
HOSP_PATH = paths.get_hospitals_path()
VACC_PATH = paths.get_vaccinations_path()


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
    df = rki_scrap.covid_daily(save_file=True)
    df_rvalue = rki_scrap.rvalue_daily(save_file=False)
    df_vaccinations_daily_states = rki_scrap.vaccinations_daily_stastes(save_file=False)

    today = dt.date.today()
    today = dt.datetime(today.year, today.month, today.day)

    # --Transformation--
    df_rki_daily = rki_transform.covid_daily(df=df, date=today)
    df_rki_daily_states = rki_transform.covid_daily_states(df=df, date=today)
    df_rki_daily_counties = rki_transform.covid_daily_counties(df=df, date=today)
    df_rki_daily_agegroups = rki_transform.covid_daily_agegroups(df=df, date=today)
    df_rki_weekly_cumulative = rki_transform.covid_weekly_cummulative(df=df)
    df_rvalue = rki_transform.rvalue_daily(df=df_rvalue)
    df_vaccinations_daily_states = rki_transform.vaccinations_daily_states(df=df_vaccinations_daily_states)

    # --DB insert--
    db.insert_or_update(df=df_rki_daily, table='covid_daily')
    db.insert_or_update(df=df_rki_daily_states, table='covid_daily_states')
    db.insert_or_update(df=df_rki_daily_counties, table='covid_daily_counties')
    db.insert_or_update(df=df_rki_daily_agegroups, table='covid_daily_agegroups')
    db.insert_or_update(df=df_rki_weekly_cumulative, table='covid_weekly_cumulative')
    db.insert_or_update(df_rvalue, table='rvalue_daily')
    db.insert_or_update(df=df_vaccinations_daily_states, table='vaccinations_daily_states')
    db.db_close()


def rki_bulk_procedure():
    db = database.ProjDB()

    for filename in os.listdir(COVID_PATH):
        if filename.endswith('.csv') or filename.endswith('.xz'):
            extract = re.search(r'\d{4}-\d{2}-\d{2}', filename)
            date = dt.datetime.strptime(extract.group(), '%Y-%m-%d')

            try:
                df = pd.read_csv(COVID_PATH + filename, engine='python', sep=',', encoding='utf8')
            except UnicodeDecodeError:
                df = pd.read_csv(COVID_PATH + filename, engine='python', sep=',', encoding='ISO-8859-1')

            # --Transformation--
            df_rki_daily = rki_transform.covid_daily(df=df, date=date)
            df_rki_daily_states = rki_transform.covid_daily_states(df=df, date=date)
            df_rki_daily_counties = rki_transform.covid_daily_counties(df=df, date=date)
            df_rki_daily_agegroups = rki_transform.covid_daily_agegroups(df=df, date=date)
            df_rki_weekly_cumulative = rki_transform.covid_weekly_cummulative(df=df)

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
