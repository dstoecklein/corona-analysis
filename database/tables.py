from sqlalchemy import Column, DateTime, Float, ForeignKey, Integer, String, null
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from config.core import cfg_table_names
from database.base_model import Base


class Agegroups05y(Base):
    __tablename__ = cfg_table_names.agegroups_05y

    agegroups_05y_id = Column(Integer, primary_key=True)
    agegroup = Column(String, nullable=False, unique=True)
    # meta cols
    created_on = Column(DateTime(timezone=True), nullable=False, default=func.now())
    updated_on = Column(DateTime(timezone=True), default=func.now())


class Agegroups10y(Base):
    __tablename__ = cfg_table_names.agegroups_10y

    agegroups_10y_id = Column(Integer, primary_key=True)
    agegroup = Column(String, nullable=False, unique=True)
    number_observations = Column(Integer)
    avg_age = Column(Float)
    # meta cols
    created_on = Column(DateTime(timezone=True), nullable=False, default=func.now())
    updated_on = Column(DateTime(timezone=True), default=func.now())


class AgegroupsRki(Base):
    __tablename__ = cfg_table_names.agegroups_rki

    agegroups_rki_id = Column(Integer, primary_key=True)
    agegroup = Column(String, nullable=False, unique=True)
    number_observations = Column(Integer)
    avg_age = Column(Float)
    # meta cols
    created_on = Column(DateTime(timezone=True), nullable=False, default=func.now())
    updated_on = Column(DateTime(timezone=True), default=func.now())


class CalendarYears(Base):
    __tablename__ = cfg_table_names.calendar_years

    calendar_years_id = Column(Integer, primary_key=True)
    iso_year = Column(Integer, nullable=False, unique=True)
    # meta cols
    created_on = Column(DateTime(timezone=True), nullable=False, default=func.now())
    updated_on = Column(DateTime(timezone=True), default=func.now())
    # relationships
    calendar_weeks: int = relationship(
        "CalendarWeeks", backref=cfg_table_names.calendar_years, cascade="all, delete"
    )


class CalendarWeeks(Base):
    __tablename__ = cfg_table_names.calendar_weeks

    calendar_weeks_id = Column(Integer, primary_key=True)
    calendar_years_fk = Column(
        Integer,
        ForeignKey(
            "_calendar_years.calendar_years_id", ondelete="CASCADE", onupdate="CASCADE"
        ),
        nullable=False,
    )
    iso_week = Column(Integer, nullable=False)
    iso_key = Column(Integer, nullable=False, unique=True)
    # meta cols
    created_on = Column(DateTime(timezone=True), nullable=False, default=func.now())
    updated_on = Column(DateTime(timezone=True), default=func.now())
    # relationships
    calendar_years: int = relationship(
        "CalendarDays", backref=cfg_table_names.calendar_weeks, cascade="all, delete"
    )


class CalendarDays(Base):
    __tablename__ = cfg_table_names.calendar_days

    calendar_days_id = Column(Integer, primary_key=True)
    calendar_weeks_fk = Column(
        Integer,
        ForeignKey(
            "_calendar_weeks.calendar_weeks_id", ondelete="CASCADE", onupdate="CASCADE"
        ),
        nullable=False,
    )
    iso_day = Column(String, nullable=False, unique=True)
    # meta cols
    created_on = Column(DateTime(timezone=True), nullable=False, default=func.now())
    updated_on = Column(DateTime(timezone=True), default=func.now())


class ClassificationsICD10(Base):
    __tablename__ = cfg_table_names.classifications_icd10

    classifications_icd10_id = Column(Integer, primary_key=True)
    icd10 = Column(String, nullable=False, unique=True)
    description_en = Column(String)
    description_de = Column(String)
    # meta cols
    created_on = Column(DateTime(timezone=True), nullable=False, default=func.now())
    updated_on = Column(DateTime(timezone=True), default=func.now())


class Countries(Base):
    __tablename__ = cfg_table_names.countries

    countries_id = Column(Integer, primary_key=True)
    country_en = Column(String, nullable=False)
    country_de = Column(String, nullable=False)
    latitude = Column(Float)
    longitude = Column(Float)
    iso_3166_1_alpha2 = Column(String(2), nullable=False, unique=True)
    iso_3166_1_alpha3 = Column(String(3), nullable=False, unique=True)
    iso_3166_1_numeric = Column(Integer, nullable=False, unique=True)
    nuts_0 = Column(String(2), nullable=False, unique=True)
    # meta cols
    created_on = Column(DateTime(timezone=True), nullable=False, default=func.now())
    updated_on = Column(DateTime(timezone=True), default=func.now())
    # relationships
    country_subdivs_1: int = relationship(
        "CountriesSubdivs1", backref=cfg_table_names.countries, cascade="all, delete"
    )


class CountriesSubdivs1(Base):
    __tablename__ = cfg_table_names.country_subdivs1

    country_subdivs_1_id = Column(Integer, primary_key=True)
    countries_fk = Column(
        Integer,
        ForeignKey(
            "_countries.countries_id", ondelete="CASCADE", onupdate="CASCADE"
        ),
        nullable=False,
    )
    subdivision_1 = Column(String, nullable=False, unique=True)
    latitude = Column(Float)
    longitude = Column(Float)
    iso_3166_2 = Column(String(5), unique=True)
    nuts_1 = Column(String(3), unique=True)
    bundesland_id = Column(Integer)
    # meta cols
    created_on = Column(DateTime(timezone=True), nullable=False, default=func.now())
    updated_on = Column(DateTime(timezone=True), default=func.now())
    # relationships
    country_subdivs_2: int = relationship(
        "CountriesSubdivs2", backref=cfg_table_names.country_subdivs1, cascade="all, delete"
    )


class CountriesSubdivs2(Base):
    __tablename__ = cfg_table_names.country_subdivs2

    country_subdivs_2_id = Column(Integer, primary_key=True)
    country_subdivs_1_fk = Column(
        Integer,
        ForeignKey(
            "_country_subdivs_1.country_subdivs_1_id", ondelete="CASCADE", onupdate="CASCADE"
        ),
        nullable=False,
    )
    subdivision_2 = Column(String, nullable=False)
    latitude = Column(Float)
    longitude = Column(Float)
    nuts_2 = Column(String(4), unique=True)
    # meta cols
    created_on = Column(DateTime(timezone=True), nullable=False, default=func.now())
    updated_on = Column(DateTime(timezone=True), default=func.now())
    # relationships
    country_subdivs_3: int = relationship(
        "CountriesSubdivs3", backref=cfg_table_names.country_subdivs2, cascade="all, delete"
    )

class CountriesSubdivs3(Base):
    __tablename__ = cfg_table_names.country_subdivs3

    country_subdivs_3_id = Column(Integer, primary_key=True)
    country_subdivs_2_fk = Column(
        Integer,
        ForeignKey(
            "_country_subdivs_2.country_subdivs_2_id", ondelete="CASCADE", onupdate="CASCADE"
        ),
        nullable=False,
    )
    subdivision_3 = Column(String, nullable=False)
    latitude = Column(Float)
    longitude = Column(Float)
    nuts_3 = Column(String(4), unique=True)
    ags = Column(Integer)
    # meta cols
    created_on = Column(DateTime(timezone=True), nullable=False, default=func.now())
    updated_on = Column(DateTime(timezone=True), default=func.now())
