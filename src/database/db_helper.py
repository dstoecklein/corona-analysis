from sqlalchemy import create_engine
from sqlalchemy.schema import MetaData
from sqlalchemy.dialects.mysql import insert
from sqlalchemy import Table
from sqlalchemy.sql import text
from src.database import config
import pandas as pd
import time


class DB:

    def __init__(self, db_name: str):
        self.engine = create_engine(
            config.dialect +
            config.username +
            ':' +
            config.password +
            '@' +
            config.ip +
            db_name
        )
        self.connection = self.engine.connect()

    def db_close(self):
        self.connection.close()

    def truncate_table(self, table_name: str):
        self.connection.execute("TRUNCATE TABLE " + table_name + ";")

    def insert_into(self, df: pd.DataFrame, table: str, replace: bool = True):
        # 1. sort df-columns in correct order
        # 2. add necessary meta columns (last_update, unique_key)
        tmp = (df.
               pipe(self.sort_columns, table).
               pipe(self.add_meta_columns)
               )

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
               )

        # create table object (sqlalchemy)
        table_obj = self.get_table_obj(table)

        # insert or update (on duplicate key)
        for index, row in tmp.iterrows():
            row_dict = row.to_dict()
            query = insert(table_obj, bind=self.engine).values(row_dict)
            update_dict = query.on_duplicate_key_update(row_dict)
            self.connection.execute(update_dict)

    @staticmethod
    def add_meta_columns(df: pd.DataFrame):
        tmp = df.copy()
        tmp['last_update'] = time.strftime('%Y-%m-%d %H:%M:%S')
        foreign_keys = [col for col in tmp if col.endswith('_fk')]
        tmp['unique_key'] = tmp[foreign_keys].apply(lambda row: '-'.join(row.values.astype(str)), axis=1)
        return tmp

    def sort_columns(self, df: pd.DataFrame, table: str):
        tmp = df.copy()
        result = self.connection.execute('SELECT * FROM ' + table)
        cols = list(result.keys())
        cols = [each_col.lower() for each_col in cols]
        cols.pop(0)

        if 'last_update' in cols:
            cols.remove('last_update')

        if 'unique_key' in cols:
            cols.remove('unique_key')

        tmp.columns = [each_col.lower() for each_col in tmp.columns]
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

    def __init__(self):
        super().__init__('rohdaten')

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


class ProjDB(DB):

    def __init__(self):
        super().__init__('corona_analysis')

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

        tmp['ID'] = tmp['ID'].astype(int)

        tmp.rename(
            columns={'ID': 'calendar_years_fk'},
            inplace=True
        )

        return tmp.drop(['iso_year', 'year'], axis=1)

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

        tmp['ID'] = tmp['ID'].astype(int)

        tmp.rename(
            columns={'ID': 'calendar_weeks_fk'},
            inplace=True
        )

        return tmp.drop(['years_fk', 'iso_week', 'iso_key'], axis=1)

    def merge_calendar_days_fk(self, df: pd.DataFrame, left_on: str):

        df_calendar_weeks = self.get_table('_calendar_days')

        try:
            df_calendar_weeks['iso_day'] = pd.to_datetime(df_calendar_weeks['iso_day'], infer_datetime_format=True)
        except (KeyError, TypeError):
            print('Error trying to convert Date columns')

        tmp = df.merge(df_calendar_weeks,
                       left_on=left_on,
                       right_on='iso_day',
                       how='left',
                       )

        if 'last_update' in tmp.columns:
            tmp = tmp.drop('last_update', axis=1)

        tmp['ID'] = tmp['ID'].astype(int)

        tmp.rename(
            columns={'ID': 'calendar_days_fk', 'weeks_fk': 'calendar_weeks_fk'},
            inplace=True
        )

        return tmp.drop(['iso_day'], axis=1)

    def merge_agegroups_fk(self, df: pd.DataFrame, left_on: str, interval: str):

        intervals = ['05y', '10y']

        if interval not in intervals:
            raise ValueError("Invalid agegroup-interval. Expected one of: {0} ".format(intervals))

        df_agegroups = None

        if interval == '05y':
            df_agegroups = self.get_table('_agegroups_05y')
        if interval == '10y':
            df_agegroups = self.get_table('_agegroups_10y')

        tmp = df.merge(df_agegroups,
                       left_on=left_on,
                       right_on='agegroup',
                       how='left',
                       )

        if 'last_update' in tmp.columns:
            tmp = tmp.drop('last_update', axis=1)

        tmp['ID'] = tmp['ID'].astype(int)

        tmp.rename(
            columns={'ID': 'agegroups_10y_fk'},
            inplace=True
        )

        return tmp.drop(['agegroup_10y', 'agegroup'], axis=1)

    def merge_classifications_icd10_fk(self, df: pd.DataFrame, left_on: str):

        df_icd10 = self.get_table('_classifications_icd10')

        tmp = df.merge(df_icd10,
                       left_on=left_on,
                       right_on='icd10',
                       how='left',
                       )

        if 'last_update' in tmp.columns:
            tmp = tmp.drop('last_update', axis=1)

        tmp['ID'] = tmp['ID'].fillna(386).astype(int)

        tmp.rename(
            columns={'ID': 'classifications_icd10_fk'},
            inplace=True
        )

        return tmp.drop(['icd10', 'description_en', 'description_de'], axis=1)

    def merge_countries_fk(self, df: pd.DataFrame, left_on: str, iso_code: str):

        iso_codes = ['alpha2', 'alpha3', 'numeric']

        if iso_code not in iso_codes:
            raise ValueError("Invalid country code standard. Expected one of: {0} ".format(iso_codes))

        df_countries = self.get_table('_countries')

        right_on = None

        if iso_code == 'alpha2':
            right_on = 'iso_3166_alpha2'
        if iso_code == 'alpha3':
            right_on = 'iso_3166_alpha3'
        if iso_code == 'numeric':
            right_on = 'iso_3166_numeric'

        df[left_on] = df[left_on].str.lower()

        tmp = df.merge(df_countries,
                       left_on=left_on,
                       right_on=right_on,
                       how='left',
                       )

        if 'last_update' in tmp.columns:
            tmp = tmp.drop('last_update', axis=1)

        tmp['ID'] = tmp['ID'].astype(int)

        tmp.rename(
            columns={'ID': 'countries_fk'},
            inplace=True
        )

        return tmp.drop(['country_en', 'country_de', 'latitude', 'longitude', 'iso_3166_alpha2',
                         'iso_3166_alpha3', 'iso_3166_numeric'], axis=1)

    def get_population(self, country: str, iso_code: str, year: str):
        iso_codes = ['alpha2', 'alpha3', 'numeric']
        col = ''

        if iso_code not in iso_codes:
            raise ValueError("Invalid country code standard. Expected one of: {0} ".format(iso_codes))

        df_countries = self.get_table('_countries')

        if iso_code == 'alpha2':
            col = 'iso_3166_alpha2'
        if iso_code == 'alpha3':
            col = 'iso_3166_alpha3'
        if iso_code == 'numeric':
            col = 'iso_3166_numeric'

        countries = df_countries[col].tolist()

        if country.lower() not in countries:
            raise ValueError("Country not found. Expected one of: {0} ".format(countries))

        query = text(
            '''
            SELECT SUM(population) AS population
            FROM population_by_agegroups
            INNER JOIN _countries ON countries_fk = _countries.ID
            INNER JOIN _calendar_years ON calendar_years_fk = _calendar_years.ID
            WHERE ''' + col + ''' = :country
            AND _calendar_years.iso_year = :year
            ;
            '''
        )
        result = self.connection.execute(query, country=country, year=year).fetchone()[0]

        return int(result)

    def get_population_by_states(self, year: str):
        query = ''
        # TODO

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
