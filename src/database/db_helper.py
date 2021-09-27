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
        df['last_update'] = time.strftime('%Y-%m-%d %H:%M:%S')

        df.columns = [each_col.lower() for each_col in df.columns]

        self.truncate_table(table)
        cols = self.get_column_names(table)
        df = df[cols]  # re-order
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

    def merge_fk(self, df: pd.DataFrame, table: str, df_fk: str, table_fk: str, drop_columns: list = None):
        df_merge = self.get_table(table)

        df = df.merge(df_merge,
                      left_on=df_fk,
                      right_on=table_fk,
                      how='left',
                      )

        if drop_columns:
            return df.drop(drop_columns, axis=1)
        return df

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
