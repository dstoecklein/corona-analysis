import uuid
from typing import Optional

import pandas as pd
from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.sql import text

from config.core import cfg_db


class Database:
    def __init__(self):
        self.engine = create_engine(f"{cfg_db.dialect}{cfg_db.name}.db", echo=True)
        metadata = MetaData()
        metadata.reflect(bind=self.engine)
        self.metadata = metadata

    def create_session(self) -> Session:
        """
        Creates and returns a new `Session` to communicate with the Database.

        Returns:
            SQLAlchemy `Session` object
        """
        Session = sessionmaker()
        Session.configure(bind=self.engine, autocommit=True)
        session = Session()
        return session

    def get_table_names(self) -> list[str]:
        """
        Returns a list of available tables

        Returns:
            list with table names as `str`
        """
        table_names = list(self.metadata.tables.keys())
        return table_names

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

    def get_pk_col_name(self, table_name: str) -> Optional[str]:
        """
        Returns the primary key of a table as `str`.

        Args:
            table_name: Name of table

        Returns:
            Column name of the primary key as `str`
        """
        if self.table_exists(table_name):
            table = self.get_table_obj(table_name)
        else:
            table = None

        if table is not None:
            primary_key = table.primary_key.columns.keys()
            if primary_key:  # if PK actually exists in the table
                return primary_key[0]
        return None

    def truncate_table(self, table_name: str) -> None:
        """
        Truncates (empties) a table.

        Args:
            table_name: Name of table
        """
        session = self.create_session()
        with session.begin():
            session.execute(text(f"""DELETE FROM {table_name};"""))
            session.commit()

    def drop_table(self, table_name: str) -> None:
        """
        Drops (deletes) a table.

        Args:
            table_name: Name of table
        """
        table = self.metadata.tables.get(table_name)
        if table is not None:
            self.metadata.drop_all(self.engine, [table], checkfirst=True)

    def upsert_df(self, df: pd.DataFrame, table_name: str) -> None:
        """
        UPSERT (UPDATE if exist, INSERT if not exist) rows of a `pandas.DataFrame``to the local SQLite database.
        Credit: https://stackoverflow.com/questions/61366664/how-to-upsert-pandas-dataframe-to-postgresql-table

        Args:
            df: `pandas.DataFrame` to be updated/inserted
            table_name: Name of table where `df` should be updated/inserted
        """

        # df must contain a unique_key column
        if "unique_key" not in df.columns:
            raise RuntimeError("DataFrame must contain a 'unique_key' column")

        # check if table exist. If not, create it using to_sql
        if not self.table_exists(table_name):
            df.to_sql(table_name, self.engine)
            return

        # table exist, so use UPSERT logic...

        # 1. create temporary table with unique id
        tmp_table = f"tmp_{uuid.uuid4().hex[:6]}"
        df.to_sql(tmp_table, self.engine, index=True)

        # 2. create column name strings
        columns = list(df.columns)
        columns_str = ", ".join(col for col in columns)

        # The "excluded." prefix causes the column to refer to the value that
        # would have been inserted if there been no conflict.
        update_columns_str = ", ".join(f"{col} = excluded.{col}" for col in columns)

        # 3. create sql query
        query_upsert = text(
            f"""
            INSERT INTO {table_name}({columns_str})
            SELECT {columns_str} FROM {tmp_table} WHERE true
            ON CONFLICT(unique_key) DO UPDATE SET
            {update_columns_str};
        """
        )

        # 4. execute upsert query & drop temporary table
        session = self.create_session()
        with session.begin():
            session.execute(query_upsert)

        self.drop_table(tmp_table)
