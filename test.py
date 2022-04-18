import pandas as pd
import datetime as dt
from config import core
from config.core import config, config_db
from utils.get_data import estat
from utils import db_helper as database

TODAY = dt.date.today()
TODAY = dt.datetime(TODAY.year, TODAY.month, TODAY.day)

# Constants
COVID_FILES_PATH = core.FILES_PATH / 'covid'
COVID_TEST_FILES_PATH = core.FILES_PATH / 'covid_tests'
COVID_RVALUE_FILES_PATH = core.FILES_PATH / 'covid_rvalue'
COVID_VACC_FILES_PATH = core.FILES_PATH / 'covid_vaccinations'
ITCU_FILES_PATH = core.FILES_PATH / 'itcus'
MORTALITIES_PATH = core.FILES_PATH / 'mortalities'
HOSPITALS_PATH = core.FILES_PATH / 'hospitals'
POPULATIONS_PATH = core.FILES_PATH / 'populations'


df_estat_life_exp = estat(
    code=config.data.estat_tables['estat_life_expectancy'],
    purpose="LIFE_EXP",
    save_file=True,
    path=POPULATIONS_PATH,
    data_type='csv'
)

def test(df: pd.DataFrame):
    db = database.DB()
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
    tmp['year'] = tmp['year'].astype(int)
    tmp = tmp[tmp["year"] >= 1990]
    tmp = db.merge_calendar_years_fk(tmp, left_on="year")
    tmp = db.merge_countries_fk(tmp, left_on="geo", country_code="iso_3166_1_alpha2")
    tmp.rename(columns={'life_expectancy': 'life_expectancy_at_birth'}, inplace=True)
    tmp.to_csv("test.csv", sep=";", index=False)
    ESTAT_LIFE_EXP_TABLE = config_db.tables["life_expectancy"]
    db.insert_or_update(df=tmp, table=ESTAT_LIFE_EXP_TABLE)
    db.db_close()
    

test(df = df_estat_life_exp)


