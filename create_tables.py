from sqlalchemy import ForeignKey, Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from db_helper2 import Database
from config.core2 import cfg_table_names

DB = Database()
Base = declarative_base()

class Agegroups_05y(Base):
   __tablename__ = cfg_table_names.agegroups_05y

   agegroups_05y_id = Column(Integer, primary_key = True)
   agegroup = Column(String, nullable=False, unique=True)
   # meta cols
   created_on = Column(DateTime(timezone=True), nullable=False, default=func.now())
   updated_on = Column(DateTime(timezone=True), default=func.now())
   unique_key = Column(String, nullable=False, unique=True)


class Agegroups_10y(Base):
   __tablename__ = cfg_table_names.agegroups_10y

   agegroups_10y_id = Column(Integer, primary_key = True)
   agegroup = Column(String, nullable=False, unique=True)
   number_observations = Column(Integer)
   avg_age = Column(Float)
   # meta cols
   created_on = Column(DateTime(timezone=True), nullable=False, default=func.now())
   updated_on = Column(DateTime(timezone=True), default=func.now())
   unique_key = Column(String, nullable=False, unique=True)


class CalendarYears(Base):
   __tablename__ = cfg_table_names.calendar_years

   calendar_years_id = Column(Integer, primary_key=True)
   iso_year = Column(Integer, nullable=False, unique=True)
   # meta cols
   created_on = Column(DateTime(timezone=True), nullable=False, default=func.now())
   updated_on = Column(DateTime(timezone=True), default=func.now())
   unique_key = Column(String, nullable=False, unique=True)
   # relationships
   calendar_weeks = relationship(
     "CalendarWeeks", 
     backref=cfg_table_names.calendar_years, 
     cascade="all, delete"
   )


class CalendarWeeks(Base):
   __tablename__ = cfg_table_names.calendar_weeks

   calendar_weeks_id = Column(Integer, primary_key=True)
   calendar_years_fk = Column(
      Integer, 
      ForeignKey("_calendar_years.calendar_years_id", 
         ondelete="CASCADE", 
         onupdate="CASCADE"
      ), 
      nullable=False)
   iso_week = Column(Integer, nullable=False)
   iso_key = Column(Integer, nullable=False, unique=True)
   # meta cols
   created_on = Column(DateTime(timezone=True), nullable=False, default=func.now())
   updated_on = Column(DateTime(timezone=True), default=func.now())
   unique_key = Column(String, nullable=False, unique=True)
   # relationships
   calendar_years = relationship(
      "CalendarDays",  
      backref=cfg_table_names.calendar_weeks, 
      cascade="all, delete"
   )


class CalendarDays(Base):
   __tablename__ = cfg_table_names.calendar_days

   calendar_days_id = Column(Integer, primary_key=True)
   calendar_weeks_fk = Column(
      Integer, 
      ForeignKey("_calendar_weeks.calendar_weeks_id", 
         ondelete="CASCADE", 
         onupdate="CASCADE"
      ), 
      nullable=False
   )
   iso_day = Column(String, nullable=False, unique=True)
   # meta cols
   created_on = Column(DateTime(timezone=True), nullable=False, default=func.now())
   updated_on = Column(DateTime(timezone=True), default=func.now())
   unique_key = Column(String, nullable=False, unique=True)


if __name__ == "__main__":
   Base.metadata.create_all(DB.engine, checkfirst=True)
