import pandas as pd
import datetime as dt
from src.utils import paths
from src.corona import rki_transform
from src.web_scraper import rki_scrap
import os
import re

"""
Runs daily via batch
"""

PATH = paths.get_covid19_ger_path()


def main():

    rki_scrap.daily_covid(save_file=True)

    for filename in os.listdir(PATH):

        if filename.endswith('.csv'):
            extract = re.search(r'\d{4}-\d{2}-\d{2}', filename)
            date = dt.datetime.strptime(extract.group(), '%Y-%m-%d')

            try:
                df = pd.read_csv(PATH + filename, engine='python', sep=',', encoding='utf8')
            except UnicodeDecodeError:
                df = pd.read_csv(PATH + filename, engine='python', sep=',', encoding='ISO-8859-1')

            # first convert to date then to datetime, because of different date values in older .csv files
            try:
                df['Meldedatum'] = pd.to_datetime(df['Meldedatum'], infer_datetime_format=True).dt.date
                df['Meldedatum'] = pd.to_datetime(df['Meldedatum'], infer_datetime_format=True)

                if 'Refdatum' in df.columns:
                    df['Refdatum'] = pd.to_datetime(df['Refdatum'], infer_datetime_format=True).dt.date
                    df['Refdatum'] = pd.to_datetime(df['Refdatum'], infer_datetime_format=True)
            except (KeyError, TypeError):
                print('Error trying to convert Date columns')

            # remove whitespaces from header
            df.columns = df.columns.str.replace(' ', '')

            #rki_transform.covid_daily(df=df, date=date, table='covid_daily')
            #rki_transform.covid_today_weekly(df=df, date=df['Meldedatum'], table='covid_today_weekly')
            rki_transform.covid_by_states_states(df=df, date=date, table='covid_daily_by_states')
            #daily_covid_cumulative_agegroups(df, date, insert_into='rki_daily_covid_agegroups_ger')


if __name__ == '__main__':
    main()
