from sqlalchemy import Column, DateTime, Float, ForeignKey, Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from config.core import cfg_db
from database.base_model import Base


def _get_created_on_col() -> Column:
    return Column(DateTime(timezone=True), nullable=False, default=func.now())


def _get_updated_on_col() -> Column:
    return Column(DateTime(timezone=True), default=func.now())


class Agegroups05y(Base):
    __tablename__ = cfg_db.tables.agegroups_05y

    agegroups_05y_id = Column(Integer, primary_key=True)
    agegroup = Column(String, nullable=False, unique=True)
    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()


class Agegroups10y(Base):
    __tablename__ = cfg_db.tables.agegroups_10y

    agegroups_10y_id = Column(Integer, primary_key=True)
    agegroup = Column(String, nullable=False, unique=True)
    number_observations = Column(Integer)
    avg_age = Column(Float)
    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()


class AgegroupsRki(Base):
    __tablename__ = cfg_db.tables.agegroups_rki

    agegroups_rki_id = Column(Integer, primary_key=True)
    agegroup = Column(String, nullable=False, unique=True)
    number_observations = Column(Integer)
    avg_age = Column(Float)
    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()


class CalendarYears(Base):
    __tablename__ = cfg_db.tables.calendar_years

    calendar_years_id = Column(Integer, primary_key=True)
    iso_year = Column(Integer, nullable=False, unique=True)
    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()
    # relationships
    calendar_weeks: int = relationship(
        "CalendarWeeks", backref=cfg_db.tables.calendar_years, cascade="all, delete"
    )


class CalendarWeeks(Base):
    __tablename__ = cfg_db.tables.calendar_weeks
    _calendar_years = CalendarYears  # parent

    calendar_weeks_id = Column(Integer, primary_key=True)
    calendar_years_fk = Column(
        Integer,
        ForeignKey(
            f"{_calendar_years.__tablename__}.{_calendar_years.calendar_years_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    iso_week = Column(Integer, nullable=False)
    iso_key = Column(Integer, nullable=False, unique=True)
    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()
    # relationships
    calendar_years: int = relationship(
        "CalendarDays", backref=cfg_db.tables.calendar_weeks, cascade="all, delete"
    )


class CalendarDays(Base):
    __tablename__ = cfg_db.tables.calendar_days
    _calendar_weeks = CalendarWeeks  # parent

    calendar_days_id = Column(Integer, primary_key=True)
    calendar_weeks_fk = Column(
        Integer,
        ForeignKey(
            f"{_calendar_weeks.__tablename__}.{_calendar_weeks.calendar_weeks_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    iso_day = Column(String, nullable=False, unique=True)
    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()


class ClassificationsICD10(Base):
    __tablename__ = cfg_db.tables.classifications_icd10

    classifications_icd10_id = Column(Integer, primary_key=True)
    icd10 = Column(String, nullable=False, unique=True)
    description_en = Column(String)
    description_de = Column(String)
    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()


class Countries(Base):
    __tablename__ = cfg_db.tables.countries

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
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()
    # relationships
    country_subdivs_1: int = relationship(
        "CountriesSubdivs1", backref=cfg_db.tables.countries, cascade="all, delete"
    )


class CountriesSubdivs1(Base):
    __tablename__ = cfg_db.tables.country_subdivs1
    _countries = Countries  # parent

    country_subdivs_1_id = Column(Integer, primary_key=True)
    countries_fk = Column(
        Integer,
        ForeignKey(
            f"{_countries.__tablename__}.{_countries.countries_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
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
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()
    # relationships
    country_subdivs_2: int = relationship(
        "CountriesSubdivs2",
        backref=cfg_db.tables.country_subdivs1,
        cascade="all, delete",
    )


class CountriesSubdivs2(Base):
    __tablename__ = cfg_db.tables.country_subdivs2
    _country_subdivs_1 = CountriesSubdivs1  # parent

    country_subdivs_2_id = Column(Integer, primary_key=True)
    country_subdivs_1_fk = Column(
        Integer,
        ForeignKey(
            f"{_country_subdivs_1.__tablename__}.{_country_subdivs_1.country_subdivs_1_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    subdivision_2 = Column(String, nullable=False)
    latitude = Column(Float)
    longitude = Column(Float)
    nuts_2 = Column(String(4), unique=True)
    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()
    # relationships
    country_subdivs_3: int = relationship(
        "CountriesSubdivs3",
        backref=cfg_db.tables.country_subdivs2,
        cascade="all, delete",
    )


class CountriesSubdivs3(Base):
    __tablename__ = cfg_db.tables.country_subdivs3
    _country_subdivs_2 = CountriesSubdivs2  # parent

    country_subdivs_3_id = Column(Integer, primary_key=True)
    country_subdivs_2_fk = Column(
        Integer,
        ForeignKey(
            f"{_country_subdivs_2.__tablename__}.{_country_subdivs_2.country_subdivs_2_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    subdivision_3 = Column(String, nullable=False)
    latitude = Column(Float)
    longitude = Column(Float)
    nuts_3 = Column(String(4), unique=True)
    ags = Column(Integer)
    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()
