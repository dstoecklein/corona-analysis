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
    # relationships
    mortality_weekly_agegroup: int = relationship(
        "MortalityWeeklyAgegroup",
        backref=cfg_db.tables.agegroup_10y,
        cascade="all, delete",
    )
    mortality_annual_agegroup_cause: int = relationship(
        "MortalityAnnualAgegroupCause",
        backref=cfg_db.tables.agegroup_10y,
        cascade="all, delete",
    )


class AgegroupRki(Base):
    __tablename__ = cfg_db.tables.agegroup_rki

    agegroup_rki_id = Column(Integer, primary_key=True)
    agegroup = Column(String, nullable=False, unique=True)
    number_observations = Column(Integer)
    avg_age = Column(Float)
    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()
    covid_daily_agegroup: int = relationship(
        "CovidDailyAgegroup",
        backref=cfg_db.tables.agegroup_rki,
        cascade="all, delete",
    )
    population_country_agegroup_rki: int = relationship(
        "PopulationCountryAgegroupRki",
        backref=cfg_db.tables.agegroup_rki,
        cascade="all, delete",
    )


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
    population_subdivision2: int = relationship(
        "PopulationSubdivision2",
        backref=cfg_db.tables.calendar_year,
        cascade="all, delete",
    )
    population_subdivision3: int = relationship(
        "PopulationSubdivision3",
        backref=cfg_db.tables.calendar_year,
        cascade="all, delete",
    )
    life_expectancy: int = relationship(
        "LifeExpectancy",
        backref=cfg_db.tables.calendar_year,
        cascade="all, delete",
    )
    median_age: int = relationship(
        "MedianAge",
        backref=cfg_db.tables.calendar_year,
        cascade="all, delete",
    )
    mortality_annual_agegroup_cause: int = relationship(
        "MortalityAnnualAgegroupCause",
        backref=cfg_db.tables.calendar_year,
        cascade="all, delete",
    )
    covid_annual: int = relationship(
        "CovidAnnual",
        backref=cfg_db.tables.calendar_year,
        cascade="all, delete",
    )
    population_country_agegroup_rki: int = relationship(
        "PopulationCountryAgegroupRki",
        backref=cfg_db.tables.calendar_year,
        cascade="all, delete",
    )
    hospital_annual: int = relationship(
        "HospitalAnnual",
        backref=cfg_db.tables.calendar_year,
        cascade="all, delete",
    )
    hospital_annual_staff: int = relationship(
        "HospitalAnnualStaff",
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
    mortality_weekly_agegroup: int = relationship(
        "MortalityWeeklyAgegroup",
        backref=cfg_db.tables.calendar_week,
        cascade="all, delete",
    )
    covid_weekly: int = relationship(
        "CovidWeekly",
        backref=cfg_db.tables.calendar_week,
        cascade="all, delete",
    )
    covid_test_weekly: int = relationship(
        "CovidTestWeekly",
        backref=cfg_db.tables.calendar_week,
        cascade="all, delete",
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
    # relationships
    vaccination_daily: int = relationship(
        "VaccinationDaily",
        backref=cfg_db.tables.calendar_day,
        cascade="all, delete",
    )
    vaccination_daily_manufacturer: int = relationship(
        "VaccinationDailyManufacturer",
        backref=cfg_db.tables.calendar_day,
        cascade="all, delete",
    )
    vaccination_daily_subdivision1: int = relationship(
        "VaccinationDailySubdivision1",
        backref=cfg_db.tables.calendar_day,
        cascade="all, delete",
    )
    itcu_daily_subdivision3: int = relationship(
        "ItcuDailySubdivision3",
        backref=cfg_db.tables.calendar_day,
        cascade="all, delete",
    )
    covid_daily: int = relationship(
        "CovidDaily",
        backref=cfg_db.tables.calendar_day,
        cascade="all, delete",
    )
    covid_daily_agegroup: int = relationship(
        "CovidDailyAgegroup",
        backref=cfg_db.tables.calendar_day,
        cascade="all, delete",
    )
    covid_daily_subdivision1: int = relationship(
        "CovidDailySubdivision1",
        backref=cfg_db.tables.calendar_day,
        cascade="all, delete",
    )
    covid_daily_subdivision3: int = relationship(
        "CovidDailySubdivision3",
        backref=cfg_db.tables.calendar_day,
        cascade="all, delete",
    )
    rvalue_daily: int = relationship(
        "RValueDaily",
        backref=cfg_db.tables.calendar_day,
        cascade="all, delete",
    )


class ClassificationICD10(Base):
    __tablename__ = cfg_db.tables.classification_icd10

    classification_icd10_id = Column(Integer, primary_key=True)
    icd10 = Column(String, nullable=False, unique=True)
    description_en = Column(String)
    description_de = Column(String)
    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()
    # relationships
    mortality_annual_agegroup_cause: int = relationship(
        "MortalityAnnualAgegroupCause",
        backref=cfg_db.tables.classification_icd10,
        cascade="all, delete",
    )


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
    life_expectancy: int = relationship(
        "LifeExpectancy", backref=cfg_db.tables.country, cascade="all, delete"
    )
    median_age: int = relationship(
        "MedianAge", backref=cfg_db.tables.country, cascade="all, delete"
    )
    mortality_weekly_agegroup: int = relationship(
        "MortalityWeeklyAgegroup", backref=cfg_db.tables.country, cascade="all, delete"
    )
    mortality_annual_agegroup_cause: int = relationship(
        "MortalityAnnualAgegroupCause",
        backref=cfg_db.tables.country,
        cascade="all, delete",
    )
    vaccination_daily: int = relationship(
        "VaccinationDaily",
        backref=cfg_db.tables.country,
        cascade="all, delete",
    )
    vaccination_daily_manufacturer: int = relationship(
        "VaccinationDailyManufacturer",
        backref=cfg_db.tables.country,
        cascade="all, delete",
    )
    covid_daily: int = relationship(
        "CovidDaily",
        backref=cfg_db.tables.country,
        cascade="all, delete",
    )
    covid_daily_agegroup: int = relationship(
        "CovidDailyAgegroup",
        backref=cfg_db.tables.country,
        cascade="all, delete",
    )
    covid_weekly: int = relationship(
        "CovidWeekly",
        backref=cfg_db.tables.country,
        cascade="all, delete",
    )
    covid_annual: int = relationship(
        "CovidAnnual",
        backref=cfg_db.tables.country,
        cascade="all, delete",
    )
    rvalue_daily: int = relationship(
        "RValueDaily",
        backref=cfg_db.tables.country,
        cascade="all, delete",
    )
    covid_test_weekly: int = relationship(
        "CovidTestWeekly",
        backref=cfg_db.tables.country,
        cascade="all, delete",
    )
    population_country_agegroup_rki: int = relationship(
        "PopulationCountryAgegroupRki",
        backref=cfg_db.tables.country,
        cascade="all, delete",
    )
    hospital_annual: int = relationship(
        "HospitalAnnual",
        backref=cfg_db.tables.country,
        cascade="all, delete",
    )
    hospital_annual_staff: int = relationship(
        "HospitalAnnualStaff",
        backref=cfg_db.tables.country,
        cascade="all, delete",
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
    vaccination_daily_subdivision1: int = relationship(
        "VaccinationDailySubdivision1",
        backref=cfg_db.tables.country_subdivision1,
        cascade="all, delete",
    )
    itcu_daily_subdivision1: int = relationship(
        "ItcuDailySubdivision1",
        backref=cfg_db.tables.country_subdivision1,
        cascade="all, delete",
    )
    covid_daily_subdivision1: int = relationship(
        "CovidDailySubdivision1",
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
    population_subdivision2: int = relationship(
        "PopulationSubdivision2",
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
    # relationships
    population_subdivision3: int = relationship(
        "PopulationSubdivision3",
        backref=cfg_db.tables.country_subdivision3,
        cascade="all, delete",
    )
    itcu_daily_subdivision3: int = relationship(
        "ItcuDailySubdivision3",
        backref=cfg_db.tables.country_subdivision3,
        cascade="all, delete",
    )
    covid_daily_subdivision3: int = relationship(
        "CovidDailySubdivision3",
        backref=cfg_db.tables.country_subdivision3,
        cascade="all, delete",
    )


class Vaccine(Base):
    __tablename__ = cfg_db.tables.vaccine

    vaccine_id = Column(Integer, primary_key=True)
    brand_name1 = Column(String, nullable=False, unique=True)
    brand_name2 = Column(String)
    manufacturer = Column(String, nullable=False)
    vaccine_type = Column(String)
    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()
    # relationships
    vaccination_daily_manufacturer: int = relationship(
        "VaccinationDailyManufacturer",
        backref=cfg_db.tables.vaccine,
        cascade="all, delete",
    )
    vaccination_daily_subdivision1: int = relationship(
        "VaccinationDailySubdivision1",
        backref=cfg_db.tables.vaccine,
        cascade="all, delete",
    )


class VaccineSeries(Base):
    __tablename__ = cfg_db.tables.vaccine_series

    vaccine_series_id = Column(Integer, primary_key=True)
    series = Column(Integer, nullable=False, unique=True)
    description = Column(String)
    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()
    # relationships
    vaccination_daily_subdivision1: int = relationship(
        "VaccinationDailySubdivision1",
        backref=cfg_db.tables.vaccine_series,
        cascade="all, delete",
    )


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

    def _uq_key(context):
        fk1 = str(context.get_current_parameters()["countries_fk"])
        fk2 = str(context.get_current_parameters()["calendar_year_fk"])
        uq = fk1 + "-" + fk2
        return uq

    unique_key = Column(String, default=_uq_key, onupdate=_uq_key, unique=True)


class PopulationCountryAgegroup(Base):
    __tablename__ = cfg_db.tables.population_country_agegroup
    _country = Country  # parent
    _calendar_year = CalendarYear  # parent
    _agegroup10y = Agegroup10y

    population_countries_agegroup_id = Column(Integer, primary_key=True)
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
    agegroup10y_fk = Column(
        Integer,
        ForeignKey(
            f"{_agegroup10y.__tablename__}.{_agegroup10y.agegroup_10y_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    population = Column(BigInteger)
    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()

    def _uq_key(context):
        fk1 = str(context.get_current_parameters()["countries_fk"])
        fk2 = str(context.get_current_parameters()["calendar_year_fk"])
        fk3 = str(context.get_current_parameters()["agegroup10y_fk"])
        uq = fk1 + "-" + fk2 + "-" + fk3
        return uq

    unique_key = Column(String, default=_uq_key, onupdate=_uq_key, unique=True)


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

    def _uq_key(context):
        fk1 = str(context.get_current_parameters()["country_subdivision1_fk"])
        fk2 = str(context.get_current_parameters()["calendar_year_fk"])
        uq = fk1 + "-" + fk2
        return uq

    unique_key = Column(String, default=_uq_key, onupdate=_uq_key, unique=True)


class PopulationSubdivision2(Base):
    __tablename__ = cfg_db.tables.population_subdivision2
    _country_subdivision2 = CountrySubdivision2
    _calendar_year = CalendarYear

    population_subdivision2_id = Column(Integer, primary_key=True)
    country_subdivision2_fk = Column(
        Integer,
        ForeignKey(
            f"{_country_subdivision2.__tablename__}.{_country_subdivision2.country_subdivision2_id.name}",
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

    def _uq_key(context):
        fk1 = str(context.get_current_parameters()["country_subdivision2_fk"])
        fk2 = str(context.get_current_parameters()["calendar_year_fk"])
        uq = fk1 + "-" + fk2
        return uq

    unique_key = Column(String, default=_uq_key, onupdate=_uq_key, unique=True)


class PopulationSubdivision3(Base):
    __tablename__ = cfg_db.tables.population_subdivision3
    _country_subdivision3 = CountrySubdivision3
    _calendar_year = CalendarYear

    population_subdivision3_id = Column(Integer, primary_key=True)
    country_subdivision3_fk = Column(
        Integer,
        ForeignKey(
            f"{_country_subdivision3.__tablename__}.{_country_subdivision3.country_subdivision3_id.name}",
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

    def _uq_key(context):
        fk1 = str(context.get_current_parameters()["country_subdivision3_fk"])
        fk2 = str(context.get_current_parameters()["calendar_year_fk"])
        uq = fk1 + "-" + fk2
        return uq

    unique_key = Column(String, default=_uq_key, onupdate=_uq_key, unique=True)


class PopulationCountryAgegroupRki(Base):
    __tablename__ = cfg_db.tables.population_country_agegroup_rki
    _country = Country
    _calendar_year = CalendarYear
    _agegroup_rki = AgegroupRki

    population_country_agegroup_rki_id = Column(Integer, primary_key=True)
    country_fk = Column(
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
    agegroup_rki_fk = Column(
        Integer,
        ForeignKey(
            f"{_agegroup_rki.__tablename__}.{_agegroup_rki.agegroup_rki_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    population = Column(BigInteger)
    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()

    def _uq_key(context):
        fk1 = str(context.get_current_parameters()["country_fk"])
        fk2 = str(context.get_current_parameters()["calendar_year_fk"])
        fk3 = str(context.get_current_parameters()["agegroup_rki_fk"])
        uq = fk1 + "-" + fk2 + "-" + fk3
        return uq

    unique_key = Column(String, default=_uq_key, onupdate=_uq_key, unique=True)


class LifeExpectancy(Base):
    __tablename__ = cfg_db.tables.life_expectancy
    _country = Country
    _calendar_year = CalendarYear

    life_expectancy_id = Column(Integer, primary_key=True)
    country_fk = Column(
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
    life_expectancy_at_birth = Column(Float)
    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()

    def _uq_key(context):
        fk1 = str(context.get_current_parameters()["country_fk"])
        fk2 = str(context.get_current_parameters()["calendar_year_fk"])
        uq = fk1 + "-" + fk2
        return uq

    unique_key = Column(String, default=_uq_key, onupdate=_uq_key, unique=True)


class MedianAge(Base):
    __tablename__ = cfg_db.tables.median_age
    _country = Country
    _calendar_year = CalendarYear

    median_age_id = Column(Integer, primary_key=True)
    country_fk = Column(
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
    median_age = Column(Float)
    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()

    def _uq_key(context):
        fk1 = str(context.get_current_parameters()["country_fk"])
        fk2 = str(context.get_current_parameters()["calendar_year_fk"])
        uq = fk1 + "-" + fk2
        return uq

    unique_key = Column(String, default=_uq_key, onupdate=_uq_key, unique=True)


class MortalityWeeklyAgegroup(Base):
    __tablename__ = cfg_db.tables.mortality_weekly_agegroup
    _country = Country
    _calendar_week = CalendarWeek
    _agegroup10y = Agegroup10y

    mortality_weekly_agegroup_id = Column(Integer, primary_key=True)
    country_fk = Column(
        Integer,
        ForeignKey(
            f"{_country.__tablename__}.{_country.country_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    calendar_week_fk = Column(
        Integer,
        ForeignKey(
            f"{_calendar_week.__tablename__}.{_calendar_week.calendar_week_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    agegroup10y_fk = Column(
        Integer,
        ForeignKey(
            f"{_agegroup10y.__tablename__}.{_agegroup10y.agegroup_10y_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    deaths = Column(Integer)

    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()

    def _uq_key(context):
        fk1 = str(context.get_current_parameters()["country_fk"])
        fk2 = str(context.get_current_parameters()["calendar_week_fk"])
        fk3 = str(context.get_current_parameters()["agegroup10y_fk"])
        uq = fk1 + "-" + fk2 + "-" + fk3
        return uq

    unique_key = Column(String, default=_uq_key, onupdate=_uq_key, unique=True)


class MortalityAnnualAgegroupCause(Base):
    __tablename__ = cfg_db.tables.mortality_annual_agegroup_cause
    _country = Country
    _calendar_year = CalendarYear
    _agegroup10y = Agegroup10y
    _classification_icd10 = ClassificationICD10

    mortality_annual_agegroup_cause_id = Column(Integer, primary_key=True)
    country_fk = Column(
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
    agegroup10y_fk = Column(
        Integer,
        ForeignKey(
            f"{_agegroup10y.__tablename__}.{_agegroup10y.agegroup_10y_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    classification_icd10_fk = Column(
        Integer,
        ForeignKey(
            f"{_classification_icd10.__tablename__}.{_classification_icd10.classification_icd10_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    deaths = Column(Integer)

    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()

    def _uq_key(context):
        fk1 = str(context.get_current_parameters()["country_fk"])
        fk2 = str(context.get_current_parameters()["calendar_year_fk"])
        fk3 = str(context.get_current_parameters()["agegroup10y_fk"])
        fk4 = str(context.get_current_parameters()["classification_icd10_fk"])
        uq = fk1 + "-" + fk2 + "-" + fk3 + "-" + fk4
        return uq

    unique_key = Column(String, default=_uq_key, onupdate=_uq_key, unique=True)


class VaccinationDaily(Base):
    __tablename__ = cfg_db.tables.vaccination_daily
    _country = Country
    _calendar_day = CalendarDay

    vaccination_daily_id = Column(Integer, primary_key=True)
    country_fk = Column(
        Integer,
        ForeignKey(
            f"{_country.__tablename__}.{_country.country_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    calendar_day_fk = Column(
        Integer,
        ForeignKey(
            f"{_calendar_day.__tablename__}.{_calendar_day.calendar_day_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    total_vaccinations = Column(BigInteger)
    first_vaccinated = Column(Integer)
    second_vaccindated = Column(Integer)
    booster_vaccinated = Column(Integer)
    new_doses_raw = Column(Integer)
    new_doses_7d_smoothed = Column(Integer)
    total_vaccinations_rate = Column(Float)
    first_vaccinated_rate = Column(Float)
    second_vaccindated_rate = Column(Float)
    booster_vaccinated_rate = Column(Float)
    daily_vaccinations_first = Column(Integer)
    daily_vaccinations_first_rate = Column(Integer)

    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()

    def _uq_key(context):
        fk1 = str(context.get_current_parameters()["country_fk"])
        fk2 = str(context.get_current_parameters()["calendar_day_fk"])
        uq = fk1 + "-" + fk2
        return uq

    unique_key = Column(String, default=_uq_key, onupdate=_uq_key, unique=True)


class VaccinationDailyManufacturer(Base):
    __tablename__ = cfg_db.tables.vaccination_daily_manufacturer
    _country = Country
    _calendar_day = CalendarDay
    _vaccine = Vaccine

    vaccination_daily_manufacturer_id = Column(Integer, primary_key=True)
    country_fk = Column(
        Integer,
        ForeignKey(
            f"{_country.__tablename__}.{_country.country_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    calendar_day_fk = Column(
        Integer,
        ForeignKey(
            f"{_calendar_day.__tablename__}.{_calendar_day.calendar_day_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    vaccine_fk = Column(
        Integer,
        ForeignKey(
            f"{_vaccine.__tablename__}.{_vaccine.vaccine_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    total_vaccinations = Column(BigInteger)
    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()

    def _uq_key(context):
        fk1 = str(context.get_current_parameters()["country_fk"])
        fk2 = str(context.get_current_parameters()["calendar_day_fk"])
        fk3 = str(context.get_current_parameters()["vaccine_fk"])
        uq = fk1 + "-" + fk2 + "-" + fk3
        return uq

    unique_key = Column(String, default=_uq_key, onupdate=_uq_key, unique=True)


class VaccinationDailySubdivision1(Base):
    __tablename__ = cfg_db.tables.vaccination_daily_subdivision1
    _country_subdivision1 = CountrySubdivision1
    _calendar_day = CalendarDay
    _vaccine = Vaccine
    _vaccine_series = VaccineSeries

    vaccination_daily_subdivision1_id = Column(Integer, primary_key=True)
    country_subdivision1_fk = Column(
        Integer,
        ForeignKey(
            f"{_country_subdivision1.__tablename__}.{_country_subdivision1.country_subdivision1_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    calendar_day_fk = Column(
        Integer,
        ForeignKey(
            f"{_calendar_day.__tablename__}.{_calendar_day.calendar_day_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    vaccine_fk = Column(
        Integer,
        ForeignKey(
            f"{_vaccine.__tablename__}.{_vaccine.vaccine_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    vaccine_series_fk = Column(
        Integer,
        ForeignKey(
            f"{_vaccine_series.__tablename__}.{_vaccine_series.vaccine_series_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    total_vaccinations = Column(BigInteger)
    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()

    def _uq_key(context):
        fk1 = str(context.get_current_parameters()["country_subdivision1_fk"])
        fk2 = str(context.get_current_parameters()["calendar_day_fk"])
        fk3 = str(context.get_current_parameters()["vaccine_fk"])
        fk4 = str(context.get_current_parameters()["vaccine_series_fk"])
        uq = fk1 + "-" + fk2 + "-" + fk3 + "-" + fk4
        return uq

    unique_key = Column(String, default=_uq_key, onupdate=_uq_key, unique=True)


class ItcuDailySubdivision1(Base):
    __tablename__ = cfg_db.tables.itcu_daily_subdivision1
    _country_subdivision1 = CountrySubdivision1
    _calendar_day = CalendarDay

    itcu_daily_subdivision1_id = Column(Integer, primary_key=True)
    country_subdivision1_fk = Column(
        Integer,
        ForeignKey(
            f"{_country_subdivision1.__tablename__}.{_country_subdivision1.country_subdivision1_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    calendar_day_fk = Column(
        Integer,
        ForeignKey(
            f"{_calendar_day.__tablename__}.{_calendar_day.calendar_day_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    treatment_group = Column(String)
    amount_reporting_areas = Column(Integer)
    cases_covid = Column(Integer)
    itcu_occupied = Column(Integer)
    itcu_free = Column(Integer)
    emergency_reserve_7d = Column(Integer)
    free_capacities_invasive_treatment = Column(Integer)
    free_capacities_invasive_treatment_covid = Column(Integer)
    operating_situation_regular = Column(Integer)
    operating_situation_partially_restricted = Column(Integer)
    operating_situation_restricted = Column(Integer)
    operating_situation_not_specified = Column(Integer)
    cases_covid_initial_reception = Column(Integer)
    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()

    def _uq_key(context):
        fk1 = str(context.get_current_parameters()["country_subdivision1_fk"])
        fk2 = str(context.get_current_parameters()["calendar_day_fk"])
        uq = fk1 + "-" + fk2
        return uq

    unique_key = Column(String, default=_uq_key, onupdate=_uq_key, unique=True)


class ItcuDailySubdivision3(Base):
    __tablename__ = cfg_db.tables.itcu_daily_subdivision3
    _country_subdivision3 = CountrySubdivision3
    _calendar_day = CalendarDay

    itcu_daily_subdivision3_id = Column(Integer, primary_key=True)
    country_subdivision3_fk = Column(
        Integer,
        ForeignKey(
            f"{_country_subdivision3.__tablename__}.{_country_subdivision3.country_subdivision3_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    calendar_day_fk = Column(
        Integer,
        ForeignKey(
            f"{_calendar_day.__tablename__}.{_calendar_day.calendar_day_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    amount_hospital_locations = Column(Integer)
    amount_reporting_areas = Column(Integer)
    cases_covid = Column(Integer)
    cases_covid_invasive_ventilated = Column(Integer)
    itcu_free = Column(Integer)
    itcu_free_adults = Column(Integer)
    itcu_occupied = Column(Integer)
    itcu_occupied_adults = Column(Integer)
    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()

    def _uq_key(context):
        fk1 = str(context.get_current_parameters()["country_subdivision3_fk"])
        fk2 = str(context.get_current_parameters()["calendar_day_fk"])
        uq = fk1 + "-" + fk2
        return uq

    unique_key = Column(String, default=_uq_key, onupdate=_uq_key, unique=True)


class CovidDaily(Base):
    __tablename__ = cfg_db.tables.covid_daily
    _country = Country
    _calendar_day = CalendarDay

    covid_daily_id = Column(Integer, primary_key=True)
    country_fk = Column(
        Integer,
        ForeignKey(
            f"{_country.__tablename__}.{_country.country_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    calendar_day_fk = Column(
        Integer,
        ForeignKey(
            f"{_calendar_day.__tablename__}.{_calendar_day.calendar_day_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    cases = Column(Integer)
    cases_delta = Column(Integer)
    cases_delta_ref = Column(Integer)
    cases_delta_ref_symptoms = Column(Integer)
    cases_7d = Column(Integer)
    cases_7d_symptoms = Column(Integer)
    cases_7d_ref = Column(Integer)
    cases_7d_ref_symptoms = Column(Integer)
    deaths = Column(Integer)
    deaths_delta = Column(Integer)
    recovered = Column(Integer)
    recovered_delta = Column(Integer)
    active_cases = Column(Integer)
    active_cases_delta = Column(Integer)
    incidence_7d = Column(Float)
    incidence_7d_symptoms = Column(Float)
    incidence_7d_ref = Column(Float)
    incidence_7d_ref_symptoms = Column(Float)
    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()

    def _uq_key(context):
        fk1 = str(context.get_current_parameters()["country_fk"])
        fk2 = str(context.get_current_parameters()["calendar_day_fk"])
        uq = fk1 + "-" + fk2
        return uq

    unique_key = Column(String, default=_uq_key, onupdate=_uq_key, unique=True)


class CovidDailyAgegroup(Base):
    __tablename__ = cfg_db.tables.covid_daily_agegroup
    _country = Country
    _calendar_day = CalendarDay
    _agegroup_rki = AgegroupRki

    covid_daily_agegroup_id = Column(Integer, primary_key=True)
    country_fk = Column(
        Integer,
        ForeignKey(
            f"{_country.__tablename__}.{_country.country_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    calendar_day_fk = Column(
        Integer,
        ForeignKey(
            f"{_calendar_day.__tablename__}.{_calendar_day.calendar_day_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    agegroup_rki_fk = Column(
        Integer,
        ForeignKey(
            f"{_agegroup_rki.__tablename__}.{_agegroup_rki.agegroup_rki_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    cases = Column(Integer)
    cases_delta = Column(Integer)
    cases_delta_ref = Column(Integer)
    cases_delta_ref_symptoms = Column(Integer)
    cases_7d = Column(Integer)
    cases_7d_symptoms = Column(Integer)
    cases_7d_ref = Column(Integer)
    cases_7d_ref_symptoms = Column(Integer)
    deaths = Column(Integer)
    deaths_delta = Column(Integer)
    recovered = Column(Integer)
    recovered_delta = Column(Integer)
    active_cases = Column(Integer)
    active_cases_delta = Column(Integer)
    incidence_7d = Column(Float)
    incidence_7d_symptoms = Column(Float)
    incidence_7d_ref = Column(Float)
    incidence_7d_ref_symptoms = Column(Float)
    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()

    def _uq_key(context):
        fk1 = str(context.get_current_parameters()["country_fk"])
        fk2 = str(context.get_current_parameters()["calendar_day_fk"])
        fk3 = str(context.get_current_parameters()["agegroup_rki_fk"])
        uq = fk1 + "-" + fk2 + "-" + fk3
        return uq

    unique_key = Column(String, default=_uq_key, onupdate=_uq_key, unique=True)


class CovidDailySubdivision1(Base):
    __tablename__ = cfg_db.tables.covid_daily_subdivision1
    _country_subdivision1 = CountrySubdivision1
    _calendar_day = CalendarDay

    covid_daily_subdivision1_id = Column(Integer, primary_key=True)
    country_subdivision1_fk = Column(
        Integer,
        ForeignKey(
            f"{_country_subdivision1.__tablename__}.{_country_subdivision1.country_subdivision1_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    calendar_day_fk = Column(
        Integer,
        ForeignKey(
            f"{_calendar_day.__tablename__}.{_calendar_day.calendar_day_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    cases = Column(Integer)
    cases_delta = Column(Integer)
    cases_delta_ref = Column(Integer)
    cases_delta_ref_symptoms = Column(Integer)
    cases_7d = Column(Integer)
    cases_7d_symptoms = Column(Integer)
    cases_7d_ref = Column(Integer)
    cases_7d_ref_symptoms = Column(Integer)
    deaths = Column(Integer)
    deaths_delta = Column(Integer)
    recovered = Column(Integer)
    recovered_delta = Column(Integer)
    active_cases = Column(Integer)
    active_cases_delta = Column(Integer)
    incidence_7d = Column(Float)
    incidence_7d_symptoms = Column(Float)
    incidence_7d_ref = Column(Float)
    incidence_7d_ref_symptoms = Column(Float)
    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()

    def _uq_key(context):
        fk1 = str(context.get_current_parameters()["country_subdivision1_fk"])
        fk2 = str(context.get_current_parameters()["calendar_day_fk"])
        uq = fk1 + "-" + fk2
        return uq

    unique_key = Column(String, default=_uq_key, onupdate=_uq_key, unique=True)


class CovidDailySubdivision3(Base):
    __tablename__ = cfg_db.tables.covid_daily_subdivision3
    _country_subdivision3 = CountrySubdivision3
    _calendar_day = CalendarDay

    covid_daily_subdivision3_id = Column(Integer, primary_key=True)
    country_subdivision3_fk = Column(
        Integer,
        ForeignKey(
            f"{_country_subdivision3.__tablename__}.{_country_subdivision3.country_subdivision3_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    calendar_day_fk = Column(
        Integer,
        ForeignKey(
            f"{_calendar_day.__tablename__}.{_calendar_day.calendar_day_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    cases = Column(Integer)
    cases_delta = Column(Integer)
    cases_delta_ref = Column(Integer)
    cases_delta_ref_symptoms = Column(Integer)
    cases_7d = Column(Integer)
    cases_7d_symptoms = Column(Integer)
    cases_7d_ref = Column(Integer)
    cases_7d_ref_symptoms = Column(Integer)
    deaths = Column(Integer)
    deaths_delta = Column(Integer)
    recovered = Column(Integer)
    recovered_delta = Column(Integer)
    active_cases = Column(Integer)
    active_cases_delta = Column(Integer)
    incidence_7d = Column(Float)
    incidence_7d_symptoms = Column(Float)
    incidence_7d_ref = Column(Float)
    incidence_7d_ref_symptoms = Column(Float)
    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()

    def _uq_key(context):
        fk1 = str(context.get_current_parameters()["country_subdivision3_fk"])
        fk2 = str(context.get_current_parameters()["calendar_day_fk"])
        uq = fk1 + "-" + fk2
        return uq

    unique_key = Column(String, default=_uq_key, onupdate=_uq_key, unique=True)


class CovidWeekly(Base):
    __tablename__ = cfg_db.tables.covid_weekly
    _country = Country
    _calendar_week = CalendarWeek

    covid_weekly_id = Column(Integer, primary_key=True)
    country_fk = Column(
        Integer,
        ForeignKey(
            f"{_country.__tablename__}.{_country.country_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    calendar_week_fk = Column(
        Integer,
        ForeignKey(
            f"{_calendar_week.__tablename__}.{_calendar_week.calendar_week_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    cases = Column(Integer)
    cases_delta = Column(Integer)
    cases_delta_ref = Column(Integer)
    cases_delta_ref_symptoms = Column(Integer)
    deaths = Column(Integer)
    deaths_delta = Column(Integer)
    recovered = Column(Integer)
    recovered_delta = Column(Integer)
    active_cases = Column(Integer)
    active_cases_delta = Column(Integer)
    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()

    def _uq_key(context):
        fk1 = str(context.get_current_parameters()["country_fk"])
        fk2 = str(context.get_current_parameters()["calendar_week_fk"])
        uq = fk1 + "-" + fk2
        return uq

    unique_key = Column(String, default=_uq_key, onupdate=_uq_key, unique=True)


class CovidAnnual(Base):
    __tablename__ = cfg_db.tables.covid_annual
    _country = Country
    _calendar_year = CalendarYear

    covid_annual_id = Column(Integer, primary_key=True)
    country_fk = Column(
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
    cases = Column(Integer)
    deaths = Column(Integer)
    recovered = Column(Integer)
    active_cases = Column(Integer)
    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()

    def _uq_key(context):
        fk1 = str(context.get_current_parameters()["country_fk"])
        fk2 = str(context.get_current_parameters()["calendar_year_fk"])
        uq = fk1 + "-" + fk2
        return uq

    unique_key = Column(String, default=_uq_key, onupdate=_uq_key, unique=True)


class RValueDaily(Base):
    __tablename__ = cfg_db.tables.rvalue_daily
    _country = Country
    _calendar_day= CalendarDay

    rvalue_daily_id = Column(Integer, primary_key=True)
    country_fk = Column(
        Integer,
        ForeignKey(
            f"{_country.__tablename__}.{_country.country_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    calendar_day_fk = Column(
        Integer,
        ForeignKey(
            f"{_calendar_day.__tablename__}.{_calendar_day.calendar_day_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    point_estimation_covid = Column(Integer)
    ll_prediction_interval_covid = Column(Integer)
    ul_prediction_interval_covid = Column(Integer)
    point_estimation_covid_smoothed = Column(Integer)
    ll_prediction_interval_covid_smoothed = Column(Integer)
    ul_prediction_interval_covid_smoothed = Column(Integer)
    point_estimation_7_day_rvalue = Column(Float)
    ll_prediction_interval_7_day_rvalue = Column(Float)
    ul_prediction_interval_7_day_rvalue = Column(Float)
    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()

    def _uq_key(context):
        fk1 = str(context.get_current_parameters()["country_fk"])
        fk2 = str(context.get_current_parameters()["calendar_day_fk"])
        uq = fk1 + "-" + fk2
        return uq

    unique_key = Column(String, default=_uq_key, onupdate=_uq_key, unique=True)


class CovidTestWeekly(Base):
    __tablename__ = cfg_db.tables.covid_test_weekly
    _country = Country
    _calendar_week = CalendarWeek

    covid_test_weekly_id = Column(Integer, primary_key=True)
    country_fk = Column(
        Integer,
        ForeignKey(
            f"{_country.__tablename__}.{_country.country_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    calendar_week_fk = Column(
        Integer,
        ForeignKey(
            f"{_calendar_week.__tablename__}.{_calendar_week.calendar_week_id.name}",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        nullable=False,
    )
    total = Column(Integer)
    positive = Column(Integer)
    positive_percentage = Column(Float)
    transferring_laboratories = Column(Integer)
    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()

    def _uq_key(context):
        fk1 = str(context.get_current_parameters()["country_fk"])
        fk2 = str(context.get_current_parameters()["calendar_week_fk"])
        uq = fk1 + "-" + fk2
        return uq

    unique_key = Column(String, default=_uq_key, onupdate=_uq_key, unique=True)


class HospitalAnnual(Base):
    __tablename__ = cfg_db.tables.hospital_annual
    _country = Country
    _calendar_year = CalendarYear

    hospital_annual_id = Column(Integer, primary_key=True)
    country_fk = Column(
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
    hospitals = Column(Integer)
    beds = Column(Integer)
    beds_per_100000 = Column(Integer)
    patients = Column(Integer)
    patients_per_100000 = Column(Integer)
    occupancy_days = Column(Integer)
    avg_days_of_hospitalization = Column(Float)
    avg_bed_occupancy_percent = Column(Float)
    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()

    def _uq_key(context):
        fk1 = str(context.get_current_parameters()["country_fk"])
        fk2 = str(context.get_current_parameters()["calendar_year_fk"])
        uq = fk1 + "-" + fk2
        return uq

    unique_key = Column(String, default=_uq_key, onupdate=_uq_key, unique=True)


class HospitalAnnualStaff(Base):
    __tablename__ = cfg_db.tables.hospital_annual_staff
    _country = Country
    _calendar_year = CalendarYear

    hospital_annual_staff_id = Column(Integer, primary_key=True)
    country_fk = Column(
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
    staff = Column(Integer)
    full_time_doctors = Column(Integer)
    non_medical_staff = Column(Integer)
    non_medical_staff_in_nursing_service = Column(Integer)
    # meta cols
    created_on = _get_created_on_col()
    updated_on = _get_updated_on_col()

    def _uq_key(context):
        fk1 = str(context.get_current_parameters()["country_fk"])
        fk2 = str(context.get_current_parameters()["calendar_year_fk"])
        uq = fk1 + "-" + fk2
        return uq

    unique_key = Column(String, default=_uq_key, onupdate=_uq_key, unique=True)
