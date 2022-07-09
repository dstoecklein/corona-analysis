import threading
import uuid
from datetime import datetime
from typing import Optional

import pandas as pd
from sqlalchemy import MetaData, Table, inspect
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

import database.tables as tbl
import database.utils as utils
from database.base_model import Base


class Database:
    _db_uri_sql_alchemy_engine_map: dict = {}
    _db_uri_sql_alchemy_engine_map_lock = threading.Lock()

    def __init__(self, db_uri: str):
        self.db_uri = db_uri

        # Quick check to see if the respective SQLAlchemy database engine has already been created.
        if db_uri not in self._db_uri_sql_alchemy_engine_map:
            with self._db_uri_sql_alchemy_engine_map_lock:
                if db_uri not in self._db_uri_sql_alchemy_engine_map:
                    self._db_uri_sql_alchemy_engine_map[
                        db_uri
                    ] = utils.create_sqlalchemy_engine(db_uri)

        self.engine = self._db_uri_sql_alchemy_engine_map[db_uri]

        # verify database emptiness by checking that 'tables' aren't
        # in the list of `expected_tables`
        # TODO: This will always re-create droped tables on object creation
        expected_tables = [
            tbl.Agegroup05y.__tablename__,
            tbl.Agegroup10y.__tablename__,
            tbl.AgegroupRki.__tablename__,
            tbl.CalendarYear.__tablename__,
            tbl.CalendarWeek.__tablename__,
            tbl.CalendarDay.__tablename__,
            tbl.ClassificationICD10.__tablename__,
            tbl.Country.__tablename__,
            tbl.CountrySubdivision1.__tablename__,
            tbl.CountrySubdivision2.__tablename__,
            tbl.CountrySubdivision3.__tablename__,
            tbl.Vaccine.__tablename__,
            tbl.VaccineSeries.__tablename__,
            tbl.PopulationCountry.__tablename__,
            tbl.PopulationSubdivision1.__tablename__,
            tbl.PopulationSubdivision2.__tablename__,
            tbl.PopulationSubdivision3.__tablename__,
            tbl.LifeExpectancy.__tablename__,
            tbl.MedianAge.__tablename__,
            tbl.MortalityWeeklyAgegroup.__tablename__,
            tbl.MortalityAnnualAgegroupCause.__tablename__,
        ]
        inspected_tables = self.get_table_names()
        if any(table not in inspected_tables for table in expected_tables):
            # Base.metadata.drop_all(self.engine) # drop all
            Base.metadata.create_all(self.engine)  # create tables

        Base.metadata.bind = self.engine
        SessionMaker = sessionmaker(bind=self.engine)
        self.ManagedSessionMaker = utils.get_managed_session_maker(SessionMaker)
        metadata = MetaData()
        metadata.reflect(bind=self.engine)
        self.metadata = metadata

    def get_table_names(self) -> set:
        """
        Returns a set of all tables

        Returns:
            `set` with table names
        """
        return set(inspect(self.engine).get_table_names())

    def table_exists(self, table_name: str) -> bool:
        """
        Returns the column name of the primary key for a given `table`.

        Args:
            table_name: Name of table

        Returns:
            Column name of the primary key as `str`
        """
        table_names = self.get_table_names()
        if table_name not in table_names:
            return False
        return True

    def get_table_obj(self, table_name: str) -> Optional[Table]:
        """
        Returns a table object

        Args:
            table_name: Name of table

        Returns:
            `sqlalchemy.Table` object
        """
        if self.table_exists(table_name):
            table = self.metadata.tables[table_name]

        if table is not None:
            return table
        return None

    def get_primary_keys(self, table_name: str) -> list[str]:
        """
        Returns a list of all primary keys in the table.

        Args:
            table_name: Name of table

        Returns:
            `list` column names with primary keys constraint
        """
        if self.table_exists(table_name):
            table = self.get_table_obj(table_name)
        else:
            raise NoSuchTableError(f"Table '{table_name}' does not exist!")

        primary_keys = list()

        if table is not None:
            inspector = inspect(self.engine)
            primary_keys = inspector.get_pk_constraint(table_name).get(
                "constrained_columns"
            )
        return primary_keys

    def get_foreign_keys(self, table_name: str) -> list[str]:
        """
        Returns a list of all foreign keys in the table.

        Args:
            table_name: Name of table

        Returns:
            `list` column names with foreign keys constraint
        """
        if self.table_exists(table_name):
            table = self.get_table_obj(table_name)
        else:
            raise NoSuchTableError(f"Table '{table_name}' does not exist!")

        foreign_keys = list()

        if table is not None:
            inspector = inspect(self.engine)
            tmp = inspector.get_foreign_keys(table_name)
            for fk in tmp:
                foreign_keys.append(fk.get("constrained_columns")[0])
        return foreign_keys

    def get_unique_cols(self, table_name: str) -> list[str]:
        """
        Returns a list of all unique columns in the table.

        Args:
            table_name: Name of table

        Returns:
            `list` column names with unique constraint
        """
        if self.table_exists(table_name):
            table = self.get_table_obj(table_name)
        else:
            raise NoSuchTableError(f"Table '{table_name}' does not exist!")

        unique_cols = list()

        if table is not None:
            inspector = inspect(self.engine)
            tmp = inspector.get_unique_constraints(table_name)
            for unique in tmp:
                unique_cols.append(unique.get("column_names")[0])
        return unique_cols

    def truncate_table(self, table_name: str) -> None:
        """
        Truncates (empties) a table.

        Args:
            table_name: Name of table
        """
        with self.ManagedSessionMaker() as session:
            session.execute(text(f"""DELETE FROM {table_name};"""))

    # def drop_table(self, table_name: str) -> None:
    #    """
    #    Drops (deletes) a table.
    #    Args:
    #        table_name: Name of table
    #    """
    #    table = self.metadata.tables.get(table_name)
    #    if table is not None:
    #        self.metadata.drop_all(self.engine, [table], checkfirst=True)

    # TODO: only update "updated_on", not "created_on"
    def upsert_df(self, df: pd.DataFrame, table_name: str) -> None:
        """
        UPSERT (UPDATE if exist, INSERT if not exist) rows of a `pandas.DataFrame``to the local SQLite database.
        Credit: https://stackoverflow.com/questions/61366664/how-to-upsert-pandas-dataframe-to-postgresql-table

        Args:
            df: `pandas.DataFrame` to be updated/inserted
            table_name: Name of table where `df` should be updated/inserted
        """
        # check if table exist. If not, raise error
        if not self.table_exists(table_name):
            raise NoSuchTableError(f"Table '{table_name}' does not exist!")

        # UPSERT requires a unique constraint
        unique_keys = self.get_unique_cols(table_name=table_name)
        if not unique_keys:
            raise RuntimeError(
                "Table must contain a unique constraint (Besides primary key constraint)!"
            )
        # simply pick the very first unique constraint from the list
        unique_key = unique_keys[0]

        if "created_on" not in df.columns:
            df["created_on"] = datetime.now()
        if "updated_on" not in df.columns:
            df["updated_on"] = datetime.now()

        # 1. create temporary table with unique id
        tmp_table = f"tmp_{uuid.uuid4().hex[:6]}"
        df.to_sql(tmp_table, self.engine, index=True)

        # 2. create column name strings
        columns = list(df.columns)
        columns_str = ", ".join(col for col in columns)

        # The "excluded." prefix causes the column to refer to the value that
        # would have been inserted if there been no conflict.
        update_columns_str = ", ".join(
            f"{col} = excluded.{col}" for col in columns if col != "created_on"
        )

        # 3. create sql query
        query_upsert = text(
            f"""
                INSERT INTO {table_name}({columns_str})
                SELECT {columns_str} FROM {tmp_table} WHERE true
                ON CONFLICT({unique_key}) DO UPDATE SET
                {update_columns_str};
            """
        )

        query_drop_tmp = text(
            f"""
                DROP TABLE {tmp_table};
            """
        )

        # 4. execute upsert query & drop temporary table
        with self.ManagedSessionMaker() as session:
            session.execute(query_upsert)
            session.execute(query_drop_tmp)
