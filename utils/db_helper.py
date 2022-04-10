import time

import numpy as np
import pandas as pd
from sqlalchemy import Table, create_engine
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.schema import MetaData
from sqlalchemy.sql import text

from config.core import config_db

class DB:
    def __init__(self):
        self.engine = create_engine(
            config_db.login['dialect'] +
            config_db.login['username'] +
            ':' +
            config_db.login['password'] +
            '@' +
            config_db.login['ip'] +
            config_db.db_name
        )
        self.connection = self.engine.connect()

    def db_close(self):
        self.connection.close()

    def truncate_table(self, table_name: str):
        self.connection.execute("TRUNCATE TABLE " + table_name + ";")

    def insert_into(self, df: pd.DataFrame, table: str, replace: bool, add_meta_columns: bool):

        if add_meta_columns:
            # 1. sort df-columns in correct order
            # 2. add necessary meta columns (last_update, unique_key)
            tmp = (df.
                   pipe(self.sort_columns, table).
                   pipe(self.add_meta_columns)
                   ).copy()
        else:
            tmp = self.sort_columns(df, table).copy()

        if replace:
            tmp.to_sql(table, self.connection, if_exists='replace', index=False)
        else:
            self.truncate_table(table)
            tmp.to_sql(table, self.connection, if_exists='append', index=False)

    def insert_or_update(self, df: pd.DataFrame, table: str):
        # 1. sort df-columns in correct order
        # 2. add necessary meta columns (last_update, unique_key)
        tmp = (df.
               pipe(self.sort_columns, table).
               pipe(self.add_meta_columns)
               ).copy()
        tmp = tmp.replace([np.inf, -np.inf], np.nan)
        tmp = tmp.fillna(0)
        # create table object (sqlalchemy)
        table_obj = self.get_table_obj(table)

        # insert or update (on duplicate key)
        # A candidate row will only be inserted if that row does not match an
        # existing primary or unique key in the table; otherwise, an UPDATE will be performed.
        # https://docs.sqlalchemy.org/en/14/dialects/mysql.html
        for index, row in tmp.iterrows():
            row_dict = row.to_dict()
            query = insert(table_obj, bind=self.engine).values(row_dict)
            update_dict = query.on_duplicate_key_update(row_dict)
            self.connection.execute(update_dict)

    @staticmethod
    def add_meta_columns(df: pd.DataFrame):
        # Create meta columns
        # last_update should be in every table
        # unique_key is a string-concatenation of all foreign-keys

        tmp = df.copy()
        tmp['last_update'] = time.strftime('%Y-%m-%d %H:%M:%S')

        foreign_keys = [col for col in tmp if col.endswith('_fk')]
        # avoid decimals
        for fk in foreign_keys:
            tmp[fk] = tmp[fk].astype(int)

        tmp['unique_key'] = tmp[foreign_keys].apply(lambda row: '-'.join(row.values.astype(str)), axis=1)

        return tmp

    # need to sort the columns in order to create correct unique_key
    def sort_columns(self, df: pd.DataFrame, table: str):
        tmp = df.copy()
        result = self.connection.execute('SELECT * FROM ' + table)
        cols = list(result.keys())
        cols = [each_col.lower() for each_col in cols]  # lower all column names
        cols.pop(0)  # get rid of ID column

        if 'last_update' in cols:
            cols.remove('last_update')

        if 'unique_key' in cols:
            cols.remove('unique_key')

        tmp.columns = [each_col.lower() for each_col in tmp.columns]  # lower column names

        # alternative to reindex, since it converts to nan for unknown columns
        # tmp.drop(columns=[col for col in tmp if col not in cols], inplace=True)
        # tmp = tmp[cols]
        # return tmp

        return tmp.reindex(columns=cols)

    def get_table_obj(self, table: str):
        meta = MetaData(bind=self.engine)
        return Table(table, meta, autoload=True, autoload_with=self.engine)

    def get_table_obj_list(self):
        meta = MetaData()
        meta.reflect(bind=self.engine)

        return meta.tables.keys()

    def get_column_names(self, table: str):
        result = self.connection.execute('SELECT * FROM ' + table)
        cols = list(result.keys())
        cols = [each_col.lower() for each_col in cols]

        return cols


class RawDB(DB):

    def __init__(self, config_db: dict):
        super().__init__(config_db=config_db)

    def get_table_df(self, table: str):
        return pd.read_sql("SELECT * FROM " + table, self.connection)

    def get_estat_annual_population(self, country_code: str):
        if country_code == 'DE':
            query = \
                '''
                SELECT * 
                FROM estat_annual_population
                WHERE geo = "''' + country_code + '''"
                AND geo != 'DE_TOT' 
                AND age != 'UNK' AND age !='TOTAL' AND age != 'Y_GE75' AND age != 'Y_GE80'
                AND sex = 'T';
                '''
        else:
            query = \
                '''
                SELECT * 
                FROM estat_annual_population
                WHERE geo = "''' + country_code + '''"
                AND age != 'UNK' AND age !='TOTAL' AND age != 'Y_GE75' AND age != 'Y_GE80'
                AND sex = 'T';
                '''

        return pd.read_sql(query, self.connection)

    def get_estat_weekly_deaths(self, country_code: str):
        """
        Retreives weekly deaths from Eurostat dataset in DB 'rohdaten'

        Parameters:
        country_code (str): Eurostat Country Code

        Returns:
        DataFrame: Weekly Deaths per Agegroup and Years
        """

        query = \
            '''
            SELECT * 
            FROM estat_weekly_deaths
            WHERE geo = "''' + country_code + '''"
            AND age != 'UNK' AND age !='TOTAL' AND age != 'Y80-89' AND age != 'Y_GE90'
            AND sex = 'T';
            '''

        return pd.read_sql(query, self.connection)

    def get_estat_annual_death_causes(self, country_code: str):
        """
        Retreives weekly deaths from Eurostat dataset in DB 'rohdaten'

        Parameters:
        country_code (str): Eurostat Country Code

        Returns:
        DataFrame: Weekly Deaths per Agegroup and Years with death causes
        """

        query = \
            '''
            SELECT * 
            FROM estat_annual_death_causes
            WHERE geo = "''' + country_code + '''"
            AND age != 'TOTAL' AND age !='Y_LT15' AND age != 'Y15-24' AND age != 'Y_LT25' AND age != 'Y_LT65' 
            AND age != 'Y_GE65' AND age != 'Y_GE85'
            AND sex = 'T';
            '''

        return pd.read_sql(query, self.connection)

    def get_owid_daily_covid(self, country_code: str):
        """
        Retreives daily covid from Our World in Data dataset in DB 'rohdaten'

        Parameters:
        country_code (str): ISO Country Code

        Returns:
        DataFrame: Daily covid data
        """

        query = \
            '''
            SELECT * 
            FROM owid_daily_covid
            WHERE iso_code = "''' + country_code + '''"
            ;
            '''

        return pd.read_sql(query, self.connection)

    def get_oecd_weekly_deaths_total(self, country_code: str):
        """
        Retreives total weekly deaths from OECD dataset in DB 'rohdaten'

        Parameters:
        country_code (str): ISO Country Code

        Returns:
        DataFrame: Weekly Deaths per week
        """

        query = \
            '''
            SELECT * 
            FROM oecd_weekly_deaths
            WHERE country = "''' + country_code + '''"
            AND gender = 'TOTAL' AND age ='TOTAL' AND variable = 'ALLCAUNB'
            ;
            '''

        return pd.read_sql(query, self.connection)

    def get_oecd_weekly_covid_deaths_total(self, country_code: str):
        """
        Retreives total weekly deaths from OECD dataset in DB 'rohdaten'

        Parameters:
        country_code (str): ISO Country Code

        Returns:
        DataFrame: Weekly Deaths per week
        """

        query = \
            '''
            SELECT * 
            FROM oecd_weekly_deaths
            WHERE country = "''' + country_code + '''"
            AND gender = 'TOTAL' AND age ='TOTAL' AND variable = 'COVIDNB'
            ;
            '''

        return pd.read_sql(query, self.connection)


# TODO: RENAME
class ProjDB(DB):

    def __init__(self):
        super().__init__()

    def get_table(self, table: str):
        return pd.read_sql("SELECT * FROM " + table, self.connection)

    def merge_calendar_years_fk(self, df: pd.DataFrame, left_on: str):

        df_calendar_years = self.get_table('_calendar_years')

        df[left_on] = df[left_on].astype(int)

        tmp = df.merge(df_calendar_years,
                       left_on=left_on,
                       right_on='iso_year',
                       how='left',
                       )

        if 'last_update' in tmp.columns:
            tmp = tmp.drop('last_update', axis=1)

        tmp['calendar_years_id'] = tmp['calendar_years_id'].astype(int)

        tmp.rename(
            columns={'calendar_years_id': 'calendar_years_fk'},
            inplace=True
        )

        return tmp.drop(['iso_year'], axis=1)

    def merge_calendar_weeks_fk(self, df: pd.DataFrame, left_on: str):

        df_calendar_weeks = self.get_table('_calendar_weeks')

        df[left_on] = df[left_on].astype(int)

        tmp = df.merge(df_calendar_weeks,
                       left_on=left_on,
                       right_on='iso_key',
                       how='left',
                       )

        if 'last_update' in tmp.columns:
            tmp = tmp.drop('last_update', axis=1)

        tmp['calendar_weeks_id'] = tmp['calendar_weeks_id'].astype(int)

        tmp.rename(
            columns={'calendar_weeks_id': 'calendar_weeks_fk'},
            inplace=True
        )

        return tmp.drop(['calendar_years_fk', 'iso_week', 'iso_key'], axis=1)

    def merge_calendar_days_fk(self, df: pd.DataFrame, left_on: str):

        df_calendar_days = self.get_table('_calendar_days')

        try:
            df_calendar_days['iso_day'] = pd.to_datetime(df_calendar_days['iso_day'], infer_datetime_format=True)
        except (KeyError, TypeError):
            print('Error trying to convert Date columns')

        tmp = df.merge(df_calendar_days,
                       left_on=left_on,
                       right_on='iso_day',
                       how='left',
                       )

        if 'last_update' in tmp.columns:
            tmp = tmp.drop('last_update', axis=1)

        tmp['calendar_days_id'] = tmp['calendar_days_id'].astype(int)

        tmp.rename(
            columns={'calendar_days_id': 'calendar_days_fk'},
            inplace=True
        )

        return tmp.drop(['iso_day'], axis=1)

    def merge_agegroups_fk(self, df: pd.DataFrame, left_on: str, interval: str):

        intervals = ['05y', '10y', 'rki']

        if interval not in intervals:
            raise ValueError("Invalid agegroup-interval. Expected one of: {0} ".format(intervals))

        df_agegroups = None

        if interval == '05y':
            df_agegroups = self.get_table('_agegroups_05y')
            df_agegroups['agegroups_05y_id'] = df_agegroups['agegroups_05y_id'].astype(int)
            df_agegroups.rename(
                columns={'agegroups_05y_id': 'agegroups_05y_fk'},
                inplace=True
            )

        if interval == '10y':
            df_agegroups = self.get_table('_agegroups_10y')
            df_agegroups['agegroups_10y_id'] = df_agegroups['agegroups_10y_id'].astype(int)
            df_agegroups.rename(
                columns={'agegroups_10y_id': 'agegroups_10y_fk'},
                inplace=True
            )

        if interval == 'rki':
            df_agegroups = self.get_table('_agegroups_rki')
            df_agegroups['agegroups_rki_id'] = df_agegroups['agegroups_rki_id'].astype(int)
            df_agegroups.rename(
                columns={'agegroups_rki_id': 'agegroups_rki_fk'},
                inplace=True
            )

        tmp = df.merge(df_agegroups,
                       left_on=left_on,
                       right_on='agegroup',
                       how='left',
                       )

        if 'last_update' in tmp.columns:
            tmp = tmp.drop('last_update', axis=1)

        return tmp.drop(['agegroup'], axis=1)

    def merge_classifications_icd10_fk(self, df: pd.DataFrame, left_on: str):

        df_icd10 = self.get_table('_classifications_icd10')

        tmp = df.merge(df_icd10,
                       left_on=left_on,
                       right_on='icd10',
                       how='left',
                       )

        if 'last_update' in tmp.columns:
            tmp = tmp.drop('last_update', axis=1)

        tmp['classifications_icd10_id'] = tmp['classifications_icd10_id'].fillna(386).astype(int)

        tmp.rename(
            columns={'classifications_icd10_id': 'classifications_icd10_fk'},
            inplace=True
        )

        return tmp.drop(['icd10', 'description_en', 'description_de'], axis=1)

    def merge_countries_fk(self, df: pd.DataFrame, left_on: str, country_code: str):

        country_codes = ['iso_3166_1_alpha2', 'iso_3166_1_alpha3', 'iso_3166_1_numeric', 'nuts_0']

        if country_code not in country_codes:
            raise ValueError("Invalid country code standard. Expected one of: {0} ".format(country_codes))

        df_countries = self.get_table('_countries')

        df = df.copy()

        if country_code == 'nuts_0':
            df[left_on] = df[left_on].str.upper()
        else:
            df[left_on] = df[left_on].str.lower()

        tmp = df.merge(df_countries,
                       left_on=left_on,
                       right_on=country_code,
                       how='left',
                       )

        if 'last_update' in tmp.columns:
            tmp = tmp.drop('last_update', axis=1)

        tmp = tmp[tmp['countries_id'].notna()]  # if no foreign key merged, then region is probably not available
        tmp['countries_id'] = tmp['countries_id'].astype(int)

        tmp.rename(
            columns={'countries_id': 'countries_fk'},
            inplace=True
        )

        return tmp

    def merge_subdivisions_fk(self, df: pd.DataFrame, left_on: str, level: int, subdiv_code: str):
        # TODO: Create map for levels: codes. So method only need 1 parameter
        levels = [1, 2, 3]
        subdiv_codes = ['subdivision_1', 'nuts_1', 'nuts_2', 'nuts_3', 'ags', 'bundesland_id']

        if level not in levels:
            raise ValueError("Invalid region level. Expected one of: {0} ".format(levels))

        if subdiv_code not in subdiv_codes:
            raise ValueError("Invalid country code standard. Expected one of: {0} ".format(subdiv_codes))

        if level == 1:
            df_subdivisions = self.get_table('_country_subdivs_1')
            df_subdivisions['country_subdivs_1_id'] = df_subdivisions['country_subdivs_1_id'].astype(int)
            df_subdivisions.rename(
                columns={'country_subdivs_1_id': 'country_subdivs_1_fk'},
                inplace=True
            )
        elif level == 2:
            df_subdivisions = self.get_table('_country_subdivs_2')
            df_subdivisions['country_subdivs_2_id'] = df_subdivisions['country_subdivs_2_id'].astype(int)
            df_subdivisions.rename(
                columns={'country_subdivs_2_id': 'country_subdivs_2_fk'},
                inplace=True
            )
        else:
            df_subdivisions = self.get_table('_country_subdivs_3')
            df_subdivisions['country_subdivs_3_id'] = df_subdivisions['country_subdivs_3_id'].astype(int)
            df_subdivisions.rename(
                columns={'country_subdivs_3_id': 'country_subdivs_3_fk'},
                inplace=True
            )

        df = df.copy()

        if (subdiv_code == 'nuts_1' or subdiv_code == 'nuts_2' or subdiv_code == 'nuts_3') and df[left_on].dtype == str:
            df[left_on] = df[left_on].str.upper()
        elif subdiv_code == 'subdivision_1':
            df[left_on] = df[left_on].str.lower()
            df_subdivisions['subdivision_1'] = df_subdivisions['subdivision_1'].str.lower()
        elif df[left_on].dtype == str:
            df[left_on] = df[left_on].str.lower()

        tmp = df.merge(df_subdivisions,
                       left_on=left_on,
                       right_on=subdiv_code,
                       how='left',
                       ).drop(['latitude', 'longitude', subdiv_code], axis=1)

        if 'last_update' in tmp.columns:
            tmp = tmp.drop('last_update', axis=1)

        return tmp

    def merge_vaccines_fk(self, df: pd.DataFrame, left_on: str):

        tmp = df.copy()
        df_vaccines = self.get_table('_vaccines')
        df_vaccines['brand_name_1'] = df_vaccines['brand_name_1'].str.lower()

        tmp[left_on] = tmp[left_on].str.lower()

        tmp[left_on].replace(
            ['pfizer', 'biontech', 'pfizer-biontech', 'pfizer/biontech'], 'Comirnaty',
            inplace=True
        )
        tmp[left_on].replace(
            'moderna', 'Spikevax',
            inplace=True
        )
        tmp[left_on].replace(
            ['johnson', 'johnson & johnson', 'johnson&johnson'], 'Janssen',
            inplace=True
        )
        tmp[left_on].replace(
            ['astrazeneca', 'astra', 'oxford/astrazeneca'], 'Vaxzevria',
            inplace=True
        )

        tmp[left_on] = tmp[left_on].str.lower()

        tmp = tmp.merge(df_vaccines,
                        left_on=left_on,
                        right_on='brand_name_1',
                        how='left',
                        )

        tmp.rename(
            columns={'vaccines_id': 'vaccines_fk'},
            inplace=True
        )

        return tmp

    def merge_vaccine_series_fk(self, df: pd.DataFrame, left_on: str):

        tmp = df.copy()
        df_vaccine_series = self.get_table('_vaccine_series')

        tmp = tmp.merge(df_vaccine_series,
                        left_on=left_on,
                        right_on='series',
                        how='left',
                        )

        tmp.rename(
            columns={'vaccine_series_id': 'vaccine_series_fk'},
            inplace=True
        )

        return tmp

    # TODO: constants in separate file (country_codes, levels, etc.)
    def get_population(self, country: str, country_code: str, level: int, year: str):
        country_codes = ['iso_3166_1_alpha2', 'iso_3166_1_alpha3', 'iso_3166_1_numeric', 'nuts_0']
        levels = [0, 1, 2, 3]  # 0: staaten, 1:bundesländer, 2:bezirk, 3.kreis

        if country_code not in country_codes:
            raise ValueError("Invalid country code standard. Expected one of: {0} ".format(country_codes))

        if level not in levels:
            raise ValueError("Invalid region level. Expected one of: {0} ".format(levels))

        df_countries = self.get_table('_countries')
        countries = df_countries[country_code].tolist()
        if country.lower() not in countries:
            raise ValueError("Country not found. Expected one of: {0} ".format(countries))

        if level == 3:
            query = text(
                '''
                SELECT * 
                FROM population_subdivs_3
                INNER JOIN _country_subdivs_3
                ON population_subdivs_3.country_subdivs_3_fk = _country_subdivs_3.country_subdivs_3_id
				INNER JOIN _country_subdivs_2
                ON _country_subdivs_3.country_subdivs_2_fk = _country_subdivs_2.country_subdivs_2_id
                INNER JOIN _country_subdivs_1
                ON _country_subdivs_2.country_subdivs_1_fk = _country_subdivs_1.country_subdivs_1_id
                INNER JOIN _countries
                ON _country_subdivs_1.countries_fk = _countries.countries_id
                INNER JOIN _calendar_years
                ON population_subdivs_3.calendar_years_fk = _calendar_years.calendar_years_id
                WHERE ''' + country_code + ''' = '{0}' 
                AND _calendar_years.iso_year = {1}
                ;
                '''.format(country, year)
            )

            df = pd.read_sql(query, self.connection) \
                [
                [
                    'population_subdivs_3_id',
                    'country_subdivs_3_fk',
                    'population',
                    'nuts_3',
                    'ags'
                ]
            ]

        elif level == 2:
            query = text(
                '''
                SELECT * 
                FROM population_subdivs_2
                INNER JOIN _country_subdivs_2
                ON population_subdivs_2.country_subdivs_2_fk = _country_subdivs_2.country_subdivs_2_id
                INNER JOIN _country_subdivs_1
                ON _country_subdivs_2.country_subdivs_1_fk = _country_subdivs_1.country_subdivs_1_id
                INNER JOIN _countries
                ON _country_subdivs_1.countries_fk = _countries.countries_id
                INNER JOIN _calendar_years
                ON population_subdivs_2.calendar_years_fk = _calendar_years.calendar_years_id
                WHERE ''' + country_code + ''' = '{0}'
                AND _calendar_years.iso_year = {1}
                ;
                '''.format(country, year)
            )

            df = pd.read_sql(query, self.connection) \
                [
                [
                    'population_subdivs_2_id',
                    'country_subdivs_2_fk',
                    'population',
                    'nuts_2'
                ]
            ]
        elif level == 1:
            query = text(
                '''
                SELECT *
                FROM population_subdivs_1
                INNER JOIN _country_subdivs_1
                ON population_subdivs_1.country_subdivs_1_fk = _country_subdivs_1.country_subdivs_1_id
                INNER JOIN _countries
                ON _country_subdivs_1.countries_fk = _countries.countries_id
                INNER JOIN _calendar_years
                ON population_subdivs_1.calendar_years_fk = _calendar_years.calendar_years_id
                WHERE ''' + country_code + ''' = '{0}'
                AND _calendar_years.iso_year = {1}
                ;
                '''.format(country, year)
            )

            df = pd.read_sql(query, self.connection) \
                [
                [
                    'population_subdivs_1_id',
                    'country_subdivs_1_fk',
                    'population',
                    'nuts_1',
                    'bundesland_id'
                ]
            ]
        else:
            query = text(
                '''
                SELECT *
                FROM population_countries
                INNER JOIN _countries
                ON population_countries.countries_fk = _countries.countries_id
                INNER JOIN _calendar_years
                ON population_countries.calendar_years_fk = _calendar_years.calendar_years_id
                WHERE ''' + country_code + ''' = '{0}'
                AND _calendar_years.iso_year = {1}
                ;
                '''.format(country, year)
            )

            df = pd.read_sql(query, self.connection) \
                [
                [
                    'population_countries_id',
                    'countries_fk',
                    'population',
                    'iso_3166_1_alpha2',
                    'iso_3166_1_alpha3',
                    'nuts_0'
                ]
            ]
        return df

    def get_population_by_agegroups(self, year: str):
        query = ''
        # TODO

    def insert_only_new_rows(self, df: pd.DataFrame, table: str):
        self.insert_and_replace(df, 'tmp')

        cols = ', '.join(self.get_column_names(table))

        query = \
            "INSERT INTO " + table + "(" + cols + ") " + \
            "SELECT " + cols + " FROM tmp t " + \
            "WHERE NOT EXISTS " + \
            "(" \
            "SELECT 1 FROM " + table + " sub " + \
            "WHERE sub.reporting_date = t.reporting_date" \
            ")" \
            ";"
        self.connection.execute(query)
