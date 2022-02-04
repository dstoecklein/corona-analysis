import datetime as dt
import pandas as pd
from src.utils import covid_helper, date_helper, rki_helper, db_helper as database

TODAY = dt.date.today()
TODAY = dt.datetime(TODAY.year, TODAY.month, TODAY.day)


def rki_daily(config: dict, config_cols: dict, config_db: dict, df: pd.DataFrame, date: dt.datetime):
    #db = database.ProjDB()
    incidence_reference_year = config['7d_incidence']['reference_year']
    tmp = df.copy()
    tmp.columns = config_cols['rki']['covid']['translation']
    tmp = covid_helper.rki_pre_process(config_cols=config_cols, df=tmp)
    tmp = covid_helper.rki_calc_numbers(config_cols=config_cols, df=tmp, date=TODAY)
    tmp = tmp.groupby('reporting_date').sum().reset_index()
    tmp['geo'] = 'DE'
    tmp = covid_helper.rki_calc_7d_incidence(
        config_cols=config_cols,
        config_db=config_db,
        df=tmp,
        level=0,
        reference_year=incidence_reference_year
    )
    tmp.to_csv("test.csv", sep=";", index=False)