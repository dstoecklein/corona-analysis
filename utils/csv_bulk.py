import datetime as dt
import pandas as pd
import os
import re
from config import core
from utils import db_helper as database
from src import covid, intensive_care_units

# Constants
COVID_FILES_PATH = core.FILES_PATH / 'covid'
COVID_TEST_FILES_PATH = core.FILES_PATH / 'covid_tests'
COVID_RVALUE_FILES_PATH = core.FILES_PATH / 'covid_rvalue'
COVID_VACC_FILES_PATH = core.FILES_PATH / 'covid_vaccinations'
ITCU_FILES_PATH = core.FILES_PATH / 'itcus'
MORTALITIES_PATH = core.FILES_PATH / 'mortalities'
HOSPITALS_PATH = core.FILES_PATH / 'hospitals'

def rki_bulk(filetype: str):
    db = database.ProjDB()

    for filename in os.listdir(COVID_FILES_PATH):
        extract = re.search(r'\d{4}-\d{2}-\d{2}', filename)
        date = dt.datetime.strptime(extract.group(), '%Y-%m-%d')
        if filetype == 'csv':
            if filename.endswith('.csv') or filename.endswith('.xz'):
                try:
                    df = pd.read_csv(COVID_FILES_PATH + filename, engine='python', sep=',', encoding='utf8')
                except UnicodeDecodeError:
                    df = pd.read_csv(COVID_FILES_PATH + filename, engine='python', sep=',', encoding='ISO-8859-1')
        elif filetype == 'ftr':
            if filename.endswith('.ftr'):
                df = pd.read_feather(COVID_FILES_PATH + filename)
        else:
            raise Exception("Invalid filetype!")
        covid.rki_daily(df=df, date=date)
        covid.rki_daily_states(df=df, date=date)
        covid.rki_daily_counties(df=df, date=date)
        covid.rki_daily_agegroups(df=df, date=date)
        covid.rki_weekly_cumulative(df=df)
    db.db_close()


def divi_bulk(filetype: str):
    db = database.ProjDB()

    for filename in os.listdir(ITCU_FILES_PATH):
        if filetype == 'csv':
            if filename.endswith('.csv') or filename.endswith('.xz'):
                try:
                    df = pd.read_csv(ITCU_FILES_PATH + filename, engine='python', sep=',', encoding='utf8')
                except UnicodeDecodeError:
                    df = pd.read_csv(ITCU_FILES_PATH + filename, engine='python', sep=',', encoding='ISO-8859-1')
        elif filetype == 'ftr':
            if filename.endswith('.ftr'):
                df = pd.read_feather(ITCU_FILES_PATH + filename)
        else:
            raise Exception("Invalid filetype!")
        
        if '_COUNTIES' in filename:
            intensive_care_units.divi_daily_counties(df=df)
        elif '_STATES' in filename:
            intensive_care_units.divi_daily_states(df=df)
        else:
            return
    db.db_close()
