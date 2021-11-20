import pandas as pd
import datetime as dt
from src.utils import paths
from src.corona import rki_transform
from src.hospitals import divi_transform
from src.web_scraper import rki_scrap, divi_scrap
import os
import re

"""
Runs daily via batch
"""

COVID_PATH = paths.get_covid19_ger_path()
HOSP_PATH = paths.get_hospitals_ger_path()

#TODO: Different procedures, maybe without reading the csv

def main():
    rki_scrap.daily_covid(save_file=True)
    divi_scrap.daily_itcu(save_file=True)

    # RKI procedure
    for filename in os.listdir(COVID_PATH):

        if filename.endswith('.csv'):
            extract = re.search(r'\d{4}-\d{2}-\d{2}', filename)
            date = dt.datetime.strptime(extract.group(), '%Y-%m-%d')

            try:
                df = pd.read_csv(COVID_PATH + filename, engine='python', sep=',', encoding='utf8')
            except UnicodeDecodeError:
                df = pd.read_csv(COVID_PATH + filename, engine='python', sep=',', encoding='ISO-8859-1')

            # first convert to date then to datetime, because of different date values in older .csv files
            try:
                if 'Meldedatum' in df.columns:  # rki
                    df['Meldedatum'] = pd.to_datetime(df['Meldedatum'], infer_datetime_format=True).dt.date
                    df['Meldedatum'] = pd.to_datetime(df['Meldedatum'], infer_datetime_format=True)
                if 'Refdatum' in df.columns:  # rki
                    df['Refdatum'] = pd.to_datetime(df['Refdatum'], infer_datetime_format=True).dt.date
                    df['Refdatum'] = pd.to_datetime(df['Refdatum'], infer_datetime_format=True)
            except (KeyError, TypeError):
                print('Error trying to convert Date columns')

            # remove whitespaces from header
            df.columns = df.columns.str.replace(' ', '')

            rki_transform.daily_covid(df=df, date=date, table='covid_daily')
            rki_transform.weekly_covid_cummulative(df=df, date=df['Meldedatum'], table='covid_weekly_cumulative')
            rki_transform.daily_covid_by_states(df=df, date=date, table='covid_daily_by_states')
            # rki_transform.covid_daily_by_agegroups(df=df, date=date, table='rki_daily_covid_agegroups_ger')

    # DIVI procedure
    for filename in os.listdir(HOSP_PATH):

        if filename.endswith('.csv'):

            try:
                df = pd.read_csv(HOSP_PATH + filename, engine='python', sep=',', encoding='utf8')
            except UnicodeDecodeError:
                df = pd.read_csv(HOSP_PATH + filename, engine='python', sep=',', encoding='ISO-8859-1')

            # first convert to date then to datetime, because of different date values in older .csv files
            try:
                if 'date' in df.columns:  # divi
                    df['date'] = pd.to_datetime(df['date'], infer_datetime_format=True).dt.date
                    df['date'] = pd.to_datetime(df['date'], infer_datetime_format=True)
            except (KeyError, TypeError):
                print('Error trying to convert Date columns')

            # remove whitespaces from header
            df.columns = df.columns.str.replace(' ', '')

            divi_transform.daily_itcu_cumulative(df=df, table='itcu_daily_cumulative')


if __name__ == '__main__':
    main()
