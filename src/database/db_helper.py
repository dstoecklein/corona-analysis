from sqlalchemy import create_engine
from sqlalchemy.schema import MetaData
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

    def insert_and_replace(self, df: pd.DataFrame, table: str):
        df['last_update'] = time.strftime('%Y-%m-%d %H:%M:%S')
        df.to_sql(table, self.connection, if_exists='replace', index=False)

    def insert_and_append(self, df: pd.DataFrame, table: str):
        cols = self.get_column_names(table)

        df.columns = [each_col.lower() for each_col in df.columns]

        df = df.reindex(columns=cols)

        df['last_update'] = time.strftime('%Y-%m-%d %H:%M:%S')

        fk_cols = [col for col in df if col.endswith('_fk')]
        df['unique_key'] = df[fk_cols].apply(lambda row: ''.join(row.values.astype(str)), axis=1).astype(int)

        self.truncate_table(table)

        df.to_sql(table, self.connection, if_exists='append', index=False)

    def get_table_names(self):
        meta = MetaData()
        meta.reflect(bind=self.engine)

        return meta.tables.keys()

    def get_column_names(self, table: str):
        result = self.connection.execute('SELECT * FROM ' + table)
        cols = list(result.keys())
        cols = [each_col.lower() for each_col in cols]
        cols.pop(0)

        if 'last_update' in cols:
            cols.remove('last_update')

        if 'unique_key' in cols:
            cols.remove('unique_key')

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

        tmp.rename(
            columns={'ID': 'calendar_years_fk'},
            inplace=True
        )

        return tmp.drop(['iso_year', 'year'], axis=1)

    def merge_agegroups_fk(self, df: pd.DataFrame, left_on: str, interval: str):

        intervals = ['05y', '10y']

        if interval not in intervals:
            raise ValueError("Invalid agegroup-interval. Expected one of: %s " % intervals)

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

        tmp.rename(
            columns={'ID': 'classifications_icd10_fk'},
            inplace=True
        )

        return tmp.drop(['icd10', 'description_en', 'description_de'], axis=1)

    def merge_countries_fk(self, df: pd.DataFrame, left_on: str, iso_code: str):

        iso_codes = ['alpha2', 'alpha3', 'numeric']

        if iso_code not in iso_codes:
            raise ValueError("Invalid agegroup-interval. Expected one of: %s " % iso_codes)

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

        tmp.rename(
            columns={'ID': 'countries_fk'},
            inplace=True
        )

        return tmp.drop(['geo', 'country_en', 'country_de', 'latitude', 'longitude', 'iso_3166_alpha2',
                         'iso_3166_alpha3', 'iso_3166_numeric'], axis=1)

    def get_population_germany(self, year: str):
        query = \
            '''
            SELECT germany_agegroups_id, SUM(population) AS population
            FROM germany_agegroups
            INNER JOIN calendar.years ON germany_agegroups.years_id = years.years_id 
            WHERE years.iso_year = ''' + year + '''
            ;
            '''
        return int(self.connection.execute(query).fetchone()[1])

    def get_population_germany_states(self, year: str):
        query = \
            '''
            SELECT states_id, SUM(population) AS population
            FROM germany_agegroups_states
            INNER JOIN calendar.years ON germany_agegroups.years_id = years.years_id 
            WHERE years.iso_year = ''' + year + '''
            GROUP BY states_id
            ;
            '''

        # save states population in dataframe
        df = pd.read_sql(query, self.connection)
        df['population'] = df['population'].astype(int)
        return df

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
