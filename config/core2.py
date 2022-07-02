import sys
from pathlib import Path

from pydantic import BaseModel
from strictyaml import YAML, load
from typing import Union

# paths
CWD = Path.cwd()
CFG_PATH = CWD / "config"
FILES_PATH = CWD / "files"

# config files
CFG_DATABASE = "cfg_database.yaml"
CFG_TABLE_NAMES = "cfg_table_names.yaml"


class CfgDatabase(BaseModel):
    dialect: str
    name: str


class CfgTables(BaseModel):
    agegroups_05y: str
    agegroups_10y: str
    calendar_years: str
    calendar_weeks: str
    calendar_days: str


def get_config_path() -> Path:
    return CFG_PATH


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


def create_cfg(model: BaseModel, file_name: str = None) -> Union[CfgDatabase, CfgTables]:
    if file_name is None:
        raise Exception("File name must be specified")

    config_file = read_config_file(file_name=file_name)

    _config = model(**config_file.data)
    return _config


cfg_db = create_cfg(model=CfgDatabase, file_name=CFG_DATABASE)
cfg_table_names = create_cfg(model=CfgTables, file_name=CFG_TABLE_NAMES)
