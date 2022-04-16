import sys
from pathlib import Path

from pydantic import BaseModel
from strictyaml import YAML, load

CWD = Path.cwd()
CONFIG_PATH = CWD / "config"
FILES_PATH = CWD / "files"
CONFIG_FILE = "config.yaml"
CONFIG_FILE_DB = "config_db.yaml"


class DataConfig(BaseModel):
    urls: dict
    estat_tables: dict
    genesis_tables: dict
    incidence_reference_year: str


class ColConfig(BaseModel):
    rki_covid_daily: dict
    rki_tests_weekly: dict
    rki_rvalue_daily: dict
    rki_vaccinations_daily_cumulative: dict
    rki_vaccinations_daily_states: dict
    divi_itcu_daily_counties: dict
    divi_itcu_daily_states: dict
    estat_deaths_weekly_agegroups: dict
    estat_death_causes_annual_agegroups: dict
    estat_population_agegroups: dict
    genesis_hospitals_annual: dict
    genesis_hospitals_staff_annual: dict


class DBConfig(BaseModel):
    db_name: str
    login: dict
    tables: dict
    cols: dict
    genesis_login: dict
    agegroup_intervals: dict


class MasterConfig(BaseModel):
    data: DataConfig
    cols: ColConfig


def get_config_path() -> Path:
    return CONFIG_PATH


def read_config_file(file_name: str, file_path: Path = None) -> YAML:
    if not file_path:
        file_path = get_config_path()

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


def create_and_validate_config(file_name: str = None) -> MasterConfig:
    if file_name is None:
        raise Exception("File name must be specified")

    config_file = read_config_file(file_name=file_name)

    _config = MasterConfig(
        data=DataConfig(**config_file.data), cols=ColConfig(**config_file.data)
    )
    return _config


# Handle DB config seperately because of sensitive information
def create_and_validate_config_db(file_name: str = None) -> DBConfig:
    if file_name is None:
        raise Exception("File name must be specified")

    config_file = read_config_file(file_name=file_name)

    _config = DBConfig(**config_file.data)
    return _config


config = create_and_validate_config(file_name=CONFIG_FILE)
config_db = create_and_validate_config_db(file_name=CONFIG_FILE_DB)
