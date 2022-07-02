from sqlalchemy import Table, MetaData, create_engine, ForeignKey, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import relationship, sessionmaker, Session
from datetime import datetime as dt
from sqlalchemy.sql import func

from config.core2 import cfg_db


class Database:
    def __init__(self):
        self.engine = create_engine(
            f'{cfg_db.dialect}{cfg_db.name}.db',
            echo=True
        )
        self.metadata = MetaData(bind=self.engine)

    def create_session(self) -> Session:
        Session = sessionmaker()
        Session.configure(bind=self.engine)
        return Session()

    def get_pk_col_name(self, table: str) -> str:
        tbl = Table(table, self.metadata, autoload=True, autoload_with=self.engine)
        pk = tbl.primary_key.columns.values[0].name
        return pk

