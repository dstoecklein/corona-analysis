import pandas as pd
from sqlalchemy.engine.row import Row
from sqlalchemy.orm import Session

import database.tables as tbl
from config.core import cfg_estat as cfg_estat
from config.core import cfg_init as init
from utils.get_data import estat as get_estat


def _cast_to_numeric_dtype(df: pd.DataFrame, as_int: bool = True) -> pd.DataFrame:
    tmp = df.copy()
    for i in tmp.columns:
        if i not in cfg_estat.alphanumeric_cols:
            tmp[i] = tmp[i].fillna(0)
            if as_int:
                tmp[i] = tmp[i].astype(int)
            else:
                tmp[i] = tmp[i].astype(float)
    tmp.rename(columns=cfg_estat.geo_map, inplace=True)
    return tmp


def _preprocess(
    df: pd.DataFrame,
    nuts_level: int,
    drop_cols: list,
    id_vars: list,
    var_name: str,
    value_name: str,
) -> pd.DataFrame:
    tmp = df.copy()
    # remove totals
    tmp = tmp[tmp[cfg_estat.age_col] != cfg_estat.agegroup_total_label]
    tmp = tmp[tmp[cfg_estat.sex_col] == cfg_estat.sex_total_label]
    # new column 'level' to indicate NUTS-level
    tmp = tmp.assign(level=tmp[cfg_estat.geo_col].str.len() - 2)
    # filter to NUTS-level
    tmp = tmp[tmp[cfg_estat.level_col].astype(int) == nuts_level]
    tmp.drop(drop_cols, axis=1, inplace=True)
    # melt by year
    tmp = tmp.melt(id_vars=id_vars, var_name=var_name, value_name=value_name)
    return tmp


def transform_population_countries(df: pd.DataFrame) -> pd.DataFrame:
    tmp = df.copy()
    tmp = _cast_to_numeric_dtype(df=tmp, as_int=True)
    tmp = _preprocess(
        df=tmp,
        nuts_level=0,
        drop_cols=["unit", "sex"],
        id_vars=["geo", "level", "age"],
        var_name="year",
        value_name="population",
    )
    # replace remaining labels with numeric values
    tmp[cfg_estat.age_col].replace(
        {
            cfg_estat.agegroup_zero_label: 0,
            cfg_estat.agegroup_open_label: 99,
            cfg_estat.agegroup_unkown_label: 999,
        },
        inplace=True,
    )

    # replace all other ages with numeric values and remove 'Y' prefix
    tmp[cfg_estat.age_col] = tmp[cfg_estat.age_col].astype(str).str.replace("Y", "")
    tmp[cfg_estat.age_col] = pd.to_numeric(tmp[cfg_estat.age_col], errors="coerce")
    # 10-year interval
    tmp["agegroup10y"] = pd.cut(
        tmp[cfg_estat.age_col],
        list(cfg_estat.agegroup_bins_10y),
        include_lowest=True,
        labels=init.from_config.agegroups_10y,
    )
    # 05-year interval
    tmp["agegroup05y"] = pd.cut(
        tmp[cfg_estat.age_col],
        list(cfg_estat.agegroup_bins_05y),
        include_lowest=True,
        labels=init.from_config.agegroups_05y,
    )
    # rki-year interval
    tmp["agegroupRki"] = pd.cut(
        tmp[cfg_estat.age_col],
        list(cfg_estat.agegroup_bins_rki),
        include_lowest=True,
        labels=init.from_config.agegroups_rki,
    )

    # merge fks
    return tmp


if __name__ == "__main__":
    df = get_estat(
        code="demo_pjan",
        purpose="POPULATION",
        save_file=False,
        data_type="csv",
        path=None,
    )
    tmp = transform_population_countries(df=df)
    tmp.to_csv("test.csv", sep=";", index=False)
