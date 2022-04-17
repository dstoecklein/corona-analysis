import pandas as pd

from config.core import config, config_db
from utils import db_helper as database

ESTAT_POPULATION_AGEGROUP_10Y_MAP = config.cols.estat_population_agegroups[
    "agegroup_10y_map"
]
ESTAT_POPULATION_COUNTRIES_TABLE = config_db.tables["population_countries"]
ESTAT_POPULATION_SUBDIVS1_TABLE = config_db.tables["population_subdivs_1"]
ESTAT_POPULATION_SUBDIVS2_TABLE = config_db.tables["population_subdivs_2"]
ESTAT_POPULATION_AGEGROUPS_TABLE = config_db.tables["population_countries_agegroups"]
ESTAT_LIFE_EXP_TABLE = config_db.tables["life_expectancy"]
ESTAT_MEDIAN_AGE_TABLE = config_db.tables["median_age"]
GENESIS_POPULATION_SUBDIVS3_TRANSLATION = config.cols.genesis_population_subdivision_3['translation']
GENESIS_POPULATION_SUBDIVS3_TABLE = config_db.tables['population_subdivs_3']


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
        ):
            tmp[i] = tmp[i].fillna(0)
            tmp[i] = tmp[i].astype(int)
    tmp.rename(columns={"geo\\time": "geo"}, inplace=True)
    return tmp


def _estat_pp_cols_float(df: pd.DataFrame) -> pd.DataFrame:
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
        ):
            tmp[i] = tmp[i].fillna(0)
            tmp[i] = tmp[i].astype(float)
    tmp.rename(columns={"geo\\time": "geo"}, inplace=True)
    return tmp


def _estat_pp_population_states(df: pd.DataFrame) -> pd.DataFrame:
    tmp = df.copy()

    # only need totals
    tmp.query(
        """
        age == 'TOTAL' \
        & sex == 'T'
        """,
        inplace=True,
    )
    # new column 'level' to indicate NUTS-level
    tmp = tmp.assign(level=tmp["geo"].str.len() - 2)
    # filter to NUTS-3
    tmp = tmp[tmp["level"].astype(int) <= 3]
    tmp.drop(["unit", "sex", "age"], axis=1, inplace=True)
    tmp = tmp.melt(id_vars=["geo", "level"], var_name="year", value_name="population")
    return tmp


def estat_population_countries(df: pd.DataFrame) -> None:
    db = database.DB()
    tmp = df.copy()
    tmp = _estat_pp_cols(df=tmp)
    tmp = _estat_pp_population_states(df=tmp)
    tmp = tmp[tmp["level"] == 0]
    tmp['year'] = tmp['year'].astpye(int)
    tmp = tmp[tmp["year"] >= 2015]
    tmp = db.merge_calendar_years_fk(tmp, left_on="year")
    tmp = db.merge_countries_fk(tmp, left_on="geo", country_code="nuts_0")
    db.insert_or_update(df=tmp, table=ESTAT_POPULATION_COUNTRIES_TABLE)
    db.db_close()


def estat_population_subdivision_1(df: pd.DataFrame) -> None:
    db = database.DB()
    tmp = df.copy()
    tmp = _estat_pp_cols(df=tmp)
    tmp = _estat_pp_population_states(df=tmp)
    tmp = tmp[tmp["level"] == 1]
    tmp['year'] = tmp['year'].astpye(int)
    tmp = tmp[tmp["year"] >= 2015]
    tmp = db.merge_calendar_years_fk(tmp, left_on="year")
    tmp = db.merge_subdivisions_fk(tmp, left_on="geo", subdiv_code="nuts_1", level=1)
    tmp = tmp[tmp["country_subdivs_1_fk"].notna()]  # if no foreign key merged, then region is probably not available
    db.insert_or_update(df=tmp, table=ESTAT_POPULATION_SUBDIVS1_TABLE)
    db.db_close()


def estat_population_subdivision_2(df: pd.DataFrame) -> None:
    db = database.DB()
    tmp = df.copy()
    tmp = _estat_pp_cols(df=tmp)
    tmp = _estat_pp_population_states(df=tmp)
    tmp = tmp[tmp["level"] == 2]
    tmp['year'] = tmp['year'].astpye(int)
    tmp = tmp[tmp["year"] >= 2015]
    tmp = db.merge_calendar_years_fk(tmp, left_on="year")
    tmp = db.merge_subdivisions_fk(tmp, left_on="geo", subdiv_code="nuts_2", level=2)
    tmp = tmp[tmp["country_subdivs_2_fk"].notna()]  # if no foreign key merged, then region is probably not available
    db.insert_or_update(df=tmp, table=ESTAT_POPULATION_SUBDIVS2_TABLE)
    db.db_close()


def estat_population_agegroups(df: pd.DataFrame) -> None:
    db = database.DB()
    tmp = df.copy()
    tmp = _estat_pp_cols(df=tmp)
    tmp = tmp.query(
        """
        geo.str.len() == 2 \
        & age != 'TOTAL' & age !='Y_GE75' & age != 'Y80-84' & age != 'Y_GE85' \
        & sex == 'T'
        """
    )

    # melting to years
    tmp = tmp.melt(
        id_vars=["age", "sex", "unit", "geo"], var_name="year", value_name="population"
    )

    # assign 10-year agegroups
    tmp = tmp.assign(
        agegroup_10y=tmp["age"].map(ESTAT_POPULATION_AGEGROUP_10Y_MAP)
    ).fillna("UNK")

    tmp["year"] = pd.to_numeric(tmp["year"], errors="coerce").astype(int)
    # selection from year
    tmp = tmp[tmp["year"] >= 2015]

    tmp = tmp.groupby(["geo", "year", "agegroup_10y"], as_index=False)[
        "population"
    ].sum()

    tmp = db.merge_agegroups_fk(tmp, left_on="agegroup_10y", interval="10y")
    tmp = db.merge_calendar_years_fk(tmp, left_on="year")
    tmp = db.merge_countries_fk(tmp, left_on="geo", country_code="iso_3166_1_alpha2")
    db.insert_or_update(df=tmp, table=ESTAT_POPULATION_AGEGROUPS_TABLE)
    db.db_close()


def estat_life_exp_at_birth(df: pd.DataFrame) -> None:
    db = database.DB()
    tmp = df.copy()
    tmp = _estat_pp_cols_float(df=tmp)

    tmp.query(
        """
        geo.str.len() == 2 \
        & age =='Y_LT1' \
        & sex == 'T'
        """,
        inplace=True,
    )

    tmp = tmp.melt(
        id_vars=["age", "sex", "unit", "geo"],
        var_name="year",
        value_name="life_expectancy",
    )
    tmp['year'] = tmp['year'].astpye(int)
    tmp = tmp[tmp["year"] >= 2015]

    tmp = db.merge_calendar_years_fk(tmp, left_on="year")
    tmp = db.merge_countries_fk(tmp, left_on="geo", country_code="iso_3166_1_alpha2")
    db.insert_or_update(df=tmp, table=ESTAT_LIFE_EXP_TABLE)
    db.db_close()


def estat_median_age(df: pd.DataFrame) -> None:
    db = database.DB()
    tmp = df.copy()
    tmp = _estat_pp_cols_float(df=tmp)

    tmp.query(
        """
        indic_de == 'MEDAGEPOP' \
        & geo.str.len() == 2
        """,
        inplace=True,
    )

    tmp = tmp.melt(
        id_vars=["indic_de", "geo"], var_name="year", value_name="median_age"
    )
    tmp['year'] = tmp['year'].astpye(int)
    tmp = tmp[tmp["year"] >= 2015]

    tmp = db.merge_calendar_years_fk(tmp, left_on="year")
    tmp = db.merge_countries_fk(tmp, left_on="geo", country_code="iso_3166_1_alpha2")
    db.insert_or_update(df=tmp, table=ESTAT_MEDIAN_AGE_TABLE)
    db.db_close()


def genesis_population_subdivision_3(df: pd.DataFrame) -> None:
    db = database.DB()
    tmp = df.copy()
    tmp = tmp.melt(
        id_vars=['index.0', 'index.1'], var_name='year', value_name='population'
    )
    tmp.rename(columns=GENESIS_POPULATION_SUBDIVS3_TRANSLATION, inplace=True)
    tmp['ags'] = tmp['ags'].astype(int)
    tmp = tmp.assign(year=tmp['year'].str.replace('Stichtag.', '', regex=True))
    tmp['year'] = pd.to_datetime(tmp['year'], errors='coerce', infer_datetime_format=True).dt.year.astype(int)
    tmp = db.merge_calendar_years_fk(tmp, left_on='year')
    tmp = db.merge_subdivisions_fk(tmp, left_on="ags", subdiv_code="ags", level=3)
    tmp = tmp[tmp["country_subdivs_3_fk"].notna()]  # if no foreign key merged, then region is probably not available
    tmp = tmp.fillna(0)
    tmp = tmp[tmp["year"] >= 2015]
    db.insert_or_update(df=tmp, table=GENESIS_POPULATION_SUBDIVS3_TABLE)
    db.db_close()