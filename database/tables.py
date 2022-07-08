from sqlalchemy import BigInteger, Column, DateTime, Float, ForeignKey, Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from config.core import cfg_db
from database.base_model import Base


def _get_created_on_col() -> Column:
    return Column(DateTime(timezone=True), nullable=False, default=func.now())


def _get_updated_on_col() -> Column:
    return Column(DateTime(timezone=True), default=func.now())


class Agegroup05y(Base):
    __tablename__ = cfg_db.tables.agegroup_05y

    agegroup_05y_id = Column(Integer, primary_key=True)
    agegroup = Column(String, nullable=False, unique=True)
    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()


class Agegroup10y(Base):
    __tablename__ = cfg_db.tables.agegroup_10y

    agegroup_10y_id = Column(Integer, primary_key=True)
    agegroup = Column(String, nullable=False, unique=True)
    number_observations = Column(Integer)
    avg_age = Column(Float)
    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()


class AgegroupRki(Base):
    __tablename__ = cfg_db.tables.agegroup_rki

    agegroup_rki_id = Column(Integer, primary_key=True)
    agegroup = Column(String, nullable=False, unique=True)
    number_observations = Column(Integer)
    avg_age = Column(Float)
    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()


class CalendarYear(Base):
    __tablename__ = cfg_db.tables.calendar_year

    calendar_year_id = Column(Integer, primary_key=True)
    iso_year = Column(Integer, nullable=False, unique=True)
    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()
    # relationships
    calendar_week: int = relationship(
        "CalendarWeek", backref=cfg_db.tables.calendar_year, cascade="all, delete"
    )
    population_country: int = relationship(
        "PopulationCountry",
        backref=cfg_db.tables.calendar_year,
        cascade="all, delete",
    )
    population_subdivision1: int = relationship(
        "PopulationSubdivision1",
        backref=cfg_db.tables.calendar_year,
        cascade="all, delete",
    )


class CalendarWeek(Base):
    __tablename__ = cfg_db.tables.calendar_week
    _calendar_year = CalendarYear  # parent

    calendar_week_id = Column(Integer, primary_key=True)
    calendar_year_fk = Column(
        Integer,
        ForeignKey(
            f"{_calendar_year.__tablename__}.{_calendar_year.calendar_year_id.name}",
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
    calendar_year: int = relationship(
        "CalendarDay", backref=cfg_db.tables.calendar_week, cascade="all, delete"
    )


class CalendarDay(Base):
    __tablename__ = cfg_db.tables.calendar_day
    _calendar_week = CalendarWeek  # parent

    calendar_day_id = Column(Integer, primary_key=True)
    calendar_week_fk = Column(
        Integer,
        ForeignKey(
            f"{_calendar_week.__tablename__}.{_calendar_week.calendar_week_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    iso_day = Column(String, nullable=False, unique=True)
    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()


class ClassificationICD10(Base):
    __tablename__ = cfg_db.tables.classification_icd10

    classification_icd10_id = Column(Integer, primary_key=True)
    icd10 = Column(String, nullable=False, unique=True)
    description_en = Column(String)
    description_de = Column(String)
    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()


class Country(Base):
    __tablename__ = cfg_db.tables.country

    country_id = Column(Integer, primary_key=True)
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
    country_subdivision1: int = relationship(
        "CountrySubdivision1", backref=cfg_db.tables.country, cascade="all, delete"
    )
    population_country: int = relationship(
        "PopulationCountry", backref=cfg_db.tables.country, cascade="all, delete"
    )


class CountrySubdivision1(Base):
    __tablename__ = cfg_db.tables.country_subdivision1
    _country = Country  # parent

    country_subdivision1_id = Column(Integer, primary_key=True)
    country_fk = Column(
        Integer,
        ForeignKey(
            f"{_country.__tablename__}.{_country.country_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    subdivision1 = Column(String, nullable=False, unique=True)
    latitude = Column(Float)
    longitude = Column(Float)
    iso_3166_2 = Column(String(5), unique=True)
    nuts_1 = Column(String(3), unique=True)
    bundesland_id = Column(Integer)
    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()
    # relationships
    country_subdivision2: int = relationship(
        "CountrySubdivision2",
        backref=cfg_db.tables.country_subdivision1,
        cascade="all, delete",
    )
    population_subdivision1: int = relationship(
        "PopulationSubdivision1",
        backref=cfg_db.tables.country_subdivision1,
        cascade="all, delete",
    )


class CountrySubdivision2(Base):
    __tablename__ = cfg_db.tables.country_subdivision2
    _country_subdivision1 = CountrySubdivision1  # parent

    country_subdivision2_id = Column(Integer, primary_key=True)
    country_subdivision1_fk = Column(
        Integer,
        ForeignKey(
            f"{_country_subdivision1.__tablename__}.{_country_subdivision1.country_subdivision1_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    subdivision2 = Column(String, nullable=False)
    latitude = Column(Float)
    longitude = Column(Float)
    nuts_2 = Column(String(4), unique=True)
    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()
    # relationships
    country_subdivision3: int = relationship(
        "CountrySubdivision3",
        backref=cfg_db.tables.country_subdivision2,
        cascade="all, delete",
    )


class CountrySubdivision3(Base):
    __tablename__ = cfg_db.tables.country_subdivision3
    _country_subdivision2 = CountrySubdivision2  # parent

    country_subdivision3_id = Column(Integer, primary_key=True)
    country_subdivision2_fk = Column(
        Integer,
        ForeignKey(
            f"{_country_subdivision2.__tablename__}.{_country_subdivision2.country_subdivision2_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    subdivision3 = Column(String, nullable=False)
    latitude = Column(Float)
    longitude = Column(Float)
    nuts_3 = Column(String(4), unique=True)
    ags = Column(Integer)
    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()


class PopulationCountry(Base):
    __tablename__ = cfg_db.tables.population_country
    _country = Country  # parent
    _calendar_year = CalendarYear  # parent

    population_countries_id = Column(Integer, primary_key=True)
    countries_fk = Column(
        Integer,
        ForeignKey(
            f"{_country.__tablename__}.{_country.country_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    calendar_year_fk = Column(
        Integer,
        ForeignKey(
            f"{_calendar_year.__tablename__}.{_calendar_year.calendar_year_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    population = Column(BigInteger)
    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()


class PopulationSubdivision1(Base):
    __tablename__ = cfg_db.tables.population_subdivision1
    _country_subdivision1 = CountrySubdivision1  # parent
    _calendar_year = CalendarYear  # parent

    population_subdivision1_id = Column(Integer, primary_key=True)
    country_subdivision1_fk = Column(
        Integer,
        ForeignKey(
            f"{_country_subdivision1.__tablename__}.{_country_subdivision1.country_subdivision1_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    calendar_year_fk = Column(
        Integer,
        ForeignKey(
            f"{_calendar_year.__tablename__}.{_calendar_year.calendar_year_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    population = Column(BigInteger)
    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()
