import datetime as dt

import pandas as pd

from config.core import config, config_db
from utils import db_helper as database

ESTAT_DEATHS_WEEKLY_AGEGROUP_10Y_MAP = config.cols.estat_deaths_weekly_agegroups[
    "agegroup_10y_map"
]
ESTAT_DEATH_CAUSES_ANNUAL_AGEGROUP_10Y_MAP = (
    config.cols.estat_death_causes_annual_agegroups["agegroup_10y_map"]
)
ESTAT_DEATHS_WEEKLY_AGEGROUPS_TABLE = config_db.tables["deaths_weekly_agegroups"]
ESTAT_DEATH_CAUSES_ANNUAL_AGEGROUPS_TABLE = config_db.tables[
    "death_causes_annual_agegroups"
]


def _estat_pp_cols(df: pd.DataFrame) -> pd.DataFrame:
    tmp = df.copy()
    for i in tmp.columns:
        if i not in (
            "age",
            "sex",
            "unit",
            "geo",
            "geo\\time",
            "icd10",
            "resid",
            "indic_de",
            "isced11",
        ):
            tmp[i] = tmp[i].fillna(0)
            tmp[i] = tmp[i].astype(int)
    tmp.rename(columns={"geo\\time": "geo"}, inplace=True)
    return tmp


def estat_deaths_weekly_agegroups(df: pd.DataFrame) -> None:
    db = database.DB()
    tmp = df.copy()
    tmp = _estat_pp_cols(df=tmp)

    # only need totals
    tmp.query(
        """
        age != 'TOTAL' & age != 'Y80-89' & age != 'Y_GE90' \
        & sex == 'T'
        """,
        inplace=True,
    )

    tmp = tmp.melt(
        id_vars=["age", "sex", "unit", "geo"], var_name="year", value_name="deaths"
    )

    # create ISO-KEY
    tmp[["iso_year", "iso_cw"]] = tmp["year"].str.split("W", expand=True)
    tmp["iso_cw"] = tmp["iso_cw"].str.zfill(2)
    tmp["iso_key"] = tmp["iso_year"] + tmp["iso_cw"]
    tmp["iso_year"] = pd.to_numeric(tmp["iso_year"], errors="coerce")
    tmp["iso_key"] = pd.to_numeric(tmp["iso_key"], errors="coerce")

    # remove week 99 rows
    tmp["iso_cw"] = tmp["iso_cw"].astype(int)
    tmp = tmp[tmp["iso_cw"] != 99]

    # tmp = tmp[tmp['iso_year'] >= 1990] # use this when building DB from scratch
    tmp = tmp[
        tmp["iso_year"] >= dt.datetime.now().year - 1
    ]  # only get last + current year

    # assign 10-year agegroups
    tmp = tmp.assign(
        agegroup_10y=tmp["age"].map(ESTAT_DEATHS_WEEKLY_AGEGROUP_10Y_MAP)
    ).fillna("UNK")

    tmp = db.merge_agegroups_fk(tmp, left_on="agegroup_10y", interval="10y")
    tmp = db.merge_calendar_weeks_fk(tmp, left_on="iso_key")
    tmp = db.merge_countries_fk(tmp, left_on="geo", country_code="iso_3166_1_alpha2")

    db.insert_or_update(df=tmp, table=ESTAT_DEATHS_WEEKLY_AGEGROUPS_TABLE)
    db.db_close()


def estat_death_causes_annual_agegroups(df: pd.DataFrame) -> None:
    db = database.DB()
    tmp = df.copy()
    tmp = _estat_pp_cols(df=tmp)

    # only need totals
    tmp.query(
        """
        age != 'TOTAL' & age !='Y_LT15' & age != 'Y15-24' & age != 'Y_LT25' & age != 'Y_LT65' \
        & age != 'Y_GE65' & age != 'Y_GE85' \
        & sex == 'T' \
        & resid == 'TOT_IN' \
        & icd10 != 'A-R_V-Y'
        """,
        inplace=True,
    )

    tmp = tmp.melt(
        id_vars=["age", "sex", "unit", "geo", "icd10", "resid"],
        var_name="year",
        value_name="deaths",
    )

    # assign 10-year agegroups
    tmp = tmp.assign(
        agegroup_10y=tmp["age"].map(ESTAT_DEATH_CAUSES_ANNUAL_AGEGROUP_10Y_MAP)
    ).fillna("UNK")

    # false icd10 categories
    tmp.loc[tmp["icd10"].str.contains("K72-K75"), "icd10"] = "K71-K77"
    tmp.loc[tmp["icd10"].str.contains("B180-B182"), "icd10"] = "B171-B182"

    tmp["year"] = pd.to_numeric(tmp["year"], errors="coerce")
    tmp = tmp[tmp["year"] >= dt.datetime.now().year - 1]  # only get last + current year

    tmp = db.merge_calendar_years_fk(tmp, left_on="year")
    tmp = db.merge_classifications_icd10_fk(tmp, left_on="icd10")
    tmp = db.merge_agegroups_fk(tmp, left_on="agegroup_10y", interval="10y")
    tmp = db.merge_countries_fk(tmp, left_on="geo", country_code="iso_3166_1_alpha2")

    tmp = tmp.groupby(
        [
            "classifications_icd10_fk",
            "agegroups_10y_fk",
            "countries_fk",
            "calendar_years_fk",
        ],
        as_index=False,
    )["deaths"].sum()

    db.insert_or_update(df=tmp, table=ESTAT_DEATH_CAUSES_ANNUAL_AGEGROUPS_TABLE)
    db.db_close()
