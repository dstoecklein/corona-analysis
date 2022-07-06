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
CFG_TABLE_NAMES = "cfg_table_names.yaml"
CFG_INIT_VALUES = "cfg_init_values.yaml"
CFG_URLS = "cfg_urls.yaml"


class DatabaseCfg(BaseModel):
    dialect: str
    name: str


class TablesCfg(BaseModel):
    calendar_years: str
    calendar_weeks: str
    calendar_days: str
    agegroups_05y: str
    agegroups_10y: str
    agegroups_rki: str
    classifications_icd10: str
    countries: str
    

class InitValuesCfg(BaseModel):
    calendar_start_year: int
    calendar_end_year: int
    agegroups_05y: list
    agegroups_10y: list
    agegroups_rki: list


class UrlsCfg(BaseModel):
    rki_covid_daily: str
    rki_tests_weekly: str
    rki_tests_weekly_states: str
    rki_rvalue_daily: str
    rki_vaccination_states: str
    rki_vaccination_rates: str
    rki_vaccinations_daily_cumulative: str
    owid_vaccinations_daily: str
    owid_vaccinations_daily_manufacturer: str
    divi_itcu_daily_counties: str
    divi_itcu_daily_states: str
    owid_covid: str
    estsat_weekly_deaths_agegroups: str
    estsat_death_causes_annual_agegroups: str
    estat_population_agegroups: str
    estat_population_nuts_2: str
    estat_life_expectancy: str
    estat_population_structure_indicators: str
    genesis_hospitals_annual: str
    genesis_hospital_staff_annual: str
    genesis_population_subdivision_3: str


def get_cfg_path() -> Path:
    return CFG_PATH


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


def create_table_names_cfg(file_name: str = None) -> TablesCfg:
    if file_name is None:
        raise Exception("File name must be specified")

    config_file = read_cfg(file_name=file_name)

    _config = TablesCfg(**config_file.data)
    return _config


def create_init_values_cfg(file_name: str = None) -> InitValuesCfg:
    if file_name is None:
        raise Exception("File name must be specified")

    config_file = read_cfg(file_name=file_name)

    _config = InitValuesCfg(**config_file.data)
    return _config


def create_urls_cfg(file_name: str = None) -> UrlsCfg:
    if file_name is None:
        raise Exception("File name must be specified")

    config_file = read_cfg(file_name=file_name)

    _config = UrlsCfg(**config_file.data)
    return _config


cfg_db = create_db_cfg(file_name=CFG_DATABASE)
cfg_table_names = create_table_names_cfg(file_name=CFG_TABLE_NAMES)
cfg_init_values = create_init_values_cfg(file_name=CFG_INIT_VALUES)
cfg_urls = create_urls_cfg(file_name=CFG_URLS)
