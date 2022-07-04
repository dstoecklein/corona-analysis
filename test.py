
from sqlalchemy import Table, MetaData
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, MetaData, String
from sqlalchemy import create_engine

from db_helper2 import Database
from tbl_helper import get_table_name
from init_tables import fill_calendars


DB = Database()


DB.drop_table("test")

