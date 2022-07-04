
from sqlalchemy import Table, MetaData
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, MetaData, String
from sqlalchemy import create_engine

from db_helper2 import Database

import agegroups_helper
import calendar_helper

DB = Database()


session = DB.create_session()

#l = ["00-04", "05-14", "15-34", "35-59", "60-79"]
#agegroups_helper.add_new_agegroup_rki(session, l)

#l = ["00-09", "10-19", "20-29", "30-39", "40-49", "50-59", "60-69", "70-79"]
#agegroups_helper.add_new_agegroup_10y(session, l)

calendar_helper.add_new_calendar_years(session, [2051])
