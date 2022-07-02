from sqlalchemy import MetaData, create_engine, ForeignKey, Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from datetime import datetime as dt
from sqlalchemy.sql import func

from config.core2 import cfg_db, cfg_table_names

class DB:
    def __init__(self):
        self.engine = create_engine(
            f'{cfg_db.dialect}{cfg_db.name}.db',
            echo=True
        )
        self.connection = self.engine.connect()

    def db_close(self):
        self.connection.close()