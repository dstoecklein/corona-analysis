import pandas as pd
from sqlalchemy.orm import Session

from config.core import cfg_estat as cfg_estat
from config.core import cfg_init as init
from table_helpers.agegroups_helper import get_agegroups10y_df, get_agegroups_rki_df
from table_helpers.calendars_helper import get_calendar_years_df
from table_helpers.countries_helper import get_countries_df, get_subdivisions1_df


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

    tmp = tmp[tmp[cfg_estat.sex_col] == cfg_estat.sex_total_label]
    # new column 'level' to indicate NUTS-level
    tmp = tmp.assign(level=tmp[cfg_estat.geo_col].str.len() - 2)
    # filter to NUTS-level
    tmp = tmp[tmp[cfg_estat.level_col].astype(int) == nuts_level]
    tmp.drop(drop_cols, axis=1, inplace=True)
    # melt by year
    tmp = tmp.melt(id_vars=id_vars, var_name=var_name, value_name=value_name)
    return tmp


def transform_population_countries(session: Session, df: pd.DataFrame) -> pd.DataFrame:
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
    df_countries = get_countries_df(session)
    df_years = get_calendar_years_df(session)

    # Total populations for countries
    tmp = tmp[tmp[cfg_estat.age_col] == cfg_estat.agegroup_total_label]
    # merge countries fk
    tmp = tmp.merge(df_countries, how="left", left_on="geo", right_on="nuts_0")
    tmp = tmp[
        tmp["country_id"].notna()
    ]  # if no foreign key merged, then region is probably not available
    # merge calendar years fk
    tmp = tmp.merge(df_years, how="left", left_on="year", right_on="iso_year")
    tmp = tmp[tmp["calendar_year_id"].notna()]
    # create unique key
    tmp.rename(
        {"country_id": "country_fk", "calendar_year_id": "calendar_year_fk"},
        axis=1,
        inplace=True,
    )
    tmp["country_fk"] = tmp["country_fk"].astype(int)
    tmp["calendar_year_fk"] = tmp["calendar_year_fk"].astype(int)

    tmp["unique_key"] = (
        tmp["country_fk"].astype(str) + "-" + tmp["calendar_year_fk"].astype(str)
    )

    tmp = tmp[["country_fk", "calendar_year_fk", "population", "unique_key"]]

    return tmp


def transform_population_countries_agegroups(
    session: Session, df: pd.DataFrame
) -> pd.DataFrame:
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
    df_countries = get_countries_df(session)
    df_years = get_calendar_years_df(session)
    df_agegroups10y_df = get_agegroups10y_df(session)

    # Population by agegroups for countries
    tmp = tmp[tmp[cfg_estat.age_col] != cfg_estat.agegroup_total_label]
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

    # merge countries fk
    tmp = tmp.merge(df_countries, how="left", left_on="geo", right_on="nuts_0")
    tmp = tmp[
        tmp["country_id"].notna()
    ]  # if no foreign key merged, then region is probably not available
    # merge calendar years fk
    tmp = tmp.merge(df_years, how="left", left_on="year", right_on="iso_year")
    tmp = tmp[tmp["calendar_year_id"].notna()]
    # merge agegroups fk
    tmp = tmp.merge(
        df_agegroups10y_df, how="left", left_on="agegroup10y", right_on="agegroup"
    )
    tmp = tmp[tmp["agegroup_10y_id"].notna()]
    # rename id to fk
    tmp.rename(
        {
            "country_id": "country_fk",
            "calendar_year_id": "calendar_year_fk",
            "agegroup_10y_id": "agegroup10y_fk",
        },
        axis=1,
        inplace=True,
    )
    tmp["country_fk"] = tmp["country_fk"].astype(int)
    tmp["calendar_year_fk"] = tmp["calendar_year_fk"].astype(int)
    tmp["agegroup10y_fk"] = tmp["agegroup10y_fk"].astype(int)

    # create unique key
    tmp["unique_key"] = (
        tmp["country_fk"].astype(str)
        + "-"
        + tmp["calendar_year_fk"].astype(str)
        + "-"
        + tmp["agegroup10y_fk"].astype(str)
    )

    tmp = tmp[
        ["country_fk", "calendar_year_fk", "agegroup10y_fk", "population", "unique_key"]
    ]
    tmp = tmp.groupby(
        ["country_fk", "calendar_year_fk", "agegroup10y_fk", "unique_key"],
        as_index=False,
    ).sum()

    return tmp


def transform_population_countries_agegroups_rki(
    session: Session, df: pd.DataFrame
) -> pd.DataFrame:
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
    df_countries = get_countries_df(session)
    df_years = get_calendar_years_df(session)
    df_agegroups_rki_df = get_agegroups_rki_df(session)

    # Population by agegroups for countries
    tmp = tmp[tmp[cfg_estat.age_col] != cfg_estat.agegroup_total_label]
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

    # rki-year interval
    tmp["agegroup_rki"] = pd.cut(
        tmp[cfg_estat.age_col],
        list(cfg_estat.agegroup_bins_rki),
        include_lowest=True,
        labels=init.from_config.agegroups_rki,
    )

    # merge countries fk
    tmp = tmp.merge(df_countries, how="left", left_on="geo", right_on="nuts_0")
    tmp = tmp[
        tmp["country_id"].notna()
    ]  # if no foreign key merged, then region is probably not available
    # merge calendar years fk
    tmp = tmp.merge(df_years, how="left", left_on="year", right_on="iso_year")
    tmp = tmp[tmp["calendar_year_id"].notna()]
    # merge agegroups fk
    tmp = tmp.merge(
        df_agegroups_rki_df, how="left", left_on="agegroup_rki", right_on="agegroup"
    )
    tmp = tmp[tmp["agegroup_rki_id"].notna()]
    # rename id to fk
    tmp.rename(
        {
            "country_id": "country_fk",
            "calendar_year_id": "calendar_year_fk",
            "agegroup_rki_id": "agegroup_rki_fk",
        },
        axis=1,
        inplace=True,
    )

    tmp["country_fk"] = tmp["country_fk"].astype(int)
    tmp["calendar_year_fk"] = tmp["calendar_year_fk"].astype(int)
    tmp["agegroup_rki_fk"] = tmp["agegroup_rki_fk"].astype(int)

    # create unique key
    tmp["unique_key"] = (
        tmp["country_fk"].astype(str)
        + "-"
        + tmp["calendar_year_fk"].astype(str)
        + "-"
        + tmp["agegroup_rki_fk"].astype(str)
    )

    tmp = tmp[
        [
            "country_fk",
            "calendar_year_fk",
            "agegroup_rki_fk",
            "population",
            "unique_key",
        ]
    ]
    tmp = tmp.groupby(
        ["country_fk", "calendar_year_fk", "agegroup_rki_fk", "unique_key"],
        as_index=False,
    ).sum()

    return tmp


def transform_population_subdivision1(session: Session, df: pd.DataFrame) -> pd.DataFrame:
    tmp = df.copy()

    tmp = _cast_to_numeric_dtype(df=tmp, as_int=True)
    tmp = _preprocess(
        df=tmp,
        nuts_level=1,
        drop_cols=["unit", "sex"],
        id_vars=["geo", "level", "age"],
        var_name="year",
        value_name="population",
    )
    df_subdivision1 = get_subdivisions1_df(session)
    df_years = get_calendar_years_df(session)

    # Total populations for countries
    tmp = tmp[tmp[cfg_estat.age_col] == cfg_estat.agegroup_total_label]
    
    # merge countries fk
    tmp = tmp.merge(df_subdivision1, how="left", left_on="geo", right_on="nuts_1")
    tmp = tmp[
        tmp["country_subdivision1_id"].notna()
    ]  # if no foreign key merged, then region is probably not available
    
    # merge calendar years fk
    tmp = tmp.merge(df_years, how="left", left_on="year", right_on="iso_year")
    tmp = tmp[tmp["calendar_year_id"].notna()]
    # create unique key
    tmp.rename(
        {"country_subdivision1_id": "country_subdivision1_fk", "calendar_year_id": "calendar_year_fk"},
        axis=1,
        inplace=True,
    )
    tmp["country_subdivision1_fk"] = tmp["country_subdivision1_fk"].astype(int)
    tmp["calendar_year_fk"] = tmp["calendar_year_fk"].astype(int)

    tmp["unique_key"] = (
        tmp["country_subdivision1_fk"].astype(str) + "-" + tmp["calendar_year_fk"].astype(str)
    )

    tmp = tmp[["country_subdivision1_fk", "calendar_year_fk", "population", "unique_key"]]
    
    return tmp

"""
if __name__ == "__main__":
    from database.db import Database
    from config.core import cfg_db, cfg_urls
    from utils.get_data import estat
    database = Database(db_uri=f"{cfg_db.db.dialect}{cfg_db.db.name}.db")
    
    df = estat(
        code=cfg_urls.estat_population_annual_nuts_2,
        purpose="POPULATION_NUTS2",
        save_file=False,
        data_type="csv",
        path=None,
    )
    with database.ManagedSessionMaker() as session:
        tmp = transform_population_subdivision1(session, df)
        #tmp.to_csv("tmp.csv", sep=";", index=False)
"""