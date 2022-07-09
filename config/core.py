import sys
from pathlib import Path

from pydantic import BaseModel
from strictyaml import YAML, load

# paths
CWD = Path.cwd()
CFG_PATH = CWD / "config"
FILES_PATH = CWD / "files"

# config files
CFG_DATABASE = "cfg_database.yaml"
CFG_TABLES = "cfg_tables.yaml"
CFG_INIT = "cfg_init.yaml"
CFG_URLS = "cfg_urls.yaml"


# CHILD CONFIGS
class FileValue(BaseModel):
    filename: str
    index_col: str


class File(BaseModel):
    classifications_icd10: FileValue
    countries: FileValue
    countries_subdivision1: FileValue
    countries_subdivision2: FileValue
    countries_subdivision3: FileValue


class Value(BaseModel):
    calendar_start_year: int
    calendar_end_year: int
    agegroups_05y: list
    agegroups_10y: list
    agegroups_rki: list
    incidence_reference_year: int
    berlin_districts: dict


class Database(BaseModel):
    dialect: str
    name: str


class Table(BaseModel):
    calendar_year: str
    calendar_week: str
    calendar_day: str
    agegroup_05y: str
    agegroup_10y: str
    agegroup_rki: str
    classification_icd10: str
    country: str
    country_subdivision1: str
    country_subdivision2: str
    country_subdivision3: str
    population_country: str
    population_country_agegroup: str
    population_subdivision1: str
    population_subdivision2: str
    population_subdivision3: str
    life_expectancy: str
    median_age: str
    

# PARENT CONFIGS
class InitCfg(BaseModel):
    from_file: File
    from_config: Value


class DatabaseCfg(BaseModel):
    db: Database
    tables: Table


class UrlCfg(BaseModel):
    rki_covid_daily: str
    rki_test_weekly: str
    rki_test_weekly_state: str
    rki_rvalue_daily: str
    rki_vaccination_daily_state: str
    rki_vaccination_daily_rate: str
    rki_vaccination_daily_cumulative: str
    owid_vaccination_daily: str
    owid_vaccination_daily_manufacturer: str
    divi_itcu_daily_countie: str
    divi_itcu_daily_state: str
    owid_covid_daily: str
    estsat_death_weekly_agegroup: str
    estsat_deathCause_annual_agegroup: str
    estat_population_annual_agegroup: str
    estat_population_annual_nuts_2: str
    estat_lifeExpectancy_annual: str
    estat_populationStructureIndicator_annual: str
    genesis_hospital_annual: str
    genesis_hospitalStaff_annual: str
    genesis_population_subdivision3: str


def get_cfg_path() -> Path:
    return CFG_PATH


def get_files_path() -> Path:
    return FILES_PATH


def read_cfg(file_name: str, file_path: Path = None) -> YAML:
    if not file_path:
        file_path = get_cfg_path()

    if file_path:
        try:
            file = open(file_path / file_name, "r")
        except Exception as e:
            print(f"open file {file_name} failed", e)
            sys.exit(1)
        else:
            return load(file.read())
    else:
        raise Exception(f"Config file {file_name} not found at {file_path}")


def create_db_cfg(file_name: str = None) -> DatabaseCfg:
    if file_name is None:
        raise Exception("File name must be specified")

    config_file = read_cfg(file_name=file_name)

    _config = DatabaseCfg(**config_file.data)
    return _config


def create_init_cfg(file_name: str = None) -> InitCfg:
    if file_name is None:
        raise Exception("File name must be specified")

    config_file = read_cfg(file_name=file_name)

    _config = InitCfg(**config_file.data)
    return _config


def create_url_cfg(file_name: str = None) -> UrlCfg:
    if file_name is None:
        raise Exception("File name must be specified")

    config_file = read_cfg(file_name=file_name)

    _config = UrlCfg(**config_file.data)
    return _config


cfg_db = create_db_cfg(file_name=CFG_DATABASE)
cfg_init = create_init_cfg(file_name=CFG_INIT)
cfg_urls = create_url_cfg(file_name=CFG_URLS)
