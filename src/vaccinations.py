import datetime as dt
import pandas as pd
from utils import db_helper as database


def rki_vaccinations_daily(config_cols: dict, config_db: dict, df: pd.DataFrame):
    db = database.ProjDB(config_db=config_db)
    tmp = df.copy()

    # col_: columns that already exist in the dataframe
    # drop_: columns that need's to be dropped from the dataframe
    # add_: columns that need's to be added to the dataframe
    translation = config_cols['rki']['vaccinations_daily']['translation']
    col_vacc_date = config_cols['rki']['vaccinations_daily']['english']['vacc_date']
    col_bundesland_id = config_cols['rki']['vaccinations_daily']['english']['bundesland_id']
    drop_cols = config_cols['rki']['vaccinations_daily']['drop']
    add_vacc_series_fk = config_cols['rki']['vaccinations_daily']['add']['vaccine_series_fk']
    add_amount = config_cols['rki']['vaccinations_daily']['add']['amount']
    replace_values = config_cols['rki']['vaccinations_daily']['replace']
    subdiv_code = config_db['mysql']['cols']['_country_subdivs_1']['bundesland_id']
    table = config_db['mysql']['tables']['vaccinations_daily']

    tmp.drop(labels=drop_cols, axis=1, inplace=True)
    tmp.columns = translation

    if col_vacc_date in tmp.columns:
        try:
            tmp[col_vacc_date] = pd.to_datetime(tmp[col_vacc_date], infer_datetime_format=True).dt.date
            tmp[col_vacc_date] = pd.to_datetime(tmp[col_vacc_date], infer_datetime_format=True)
        except (KeyError, TypeError):
            print('Error trying to convert Date columns')

    tmp = tmp[tmp[col_bundesland_id] != 0]
    tmp = tmp.fillna(0)
    tmp = tmp.melt(
        id_vars=[col_vacc_date, col_bundesland_id],
        var_name=add_vacc_series_fk,
        value_name=add_amount
    )

    tmp = tmp.replace({add_vacc_series_fk: replace_values})

    tmp = db.merge_subdivisions_fk(df=tmp, left_on=col_bundesland_id, level=1, subdiv_code=subdiv_code)
    tmp = db.merge_calendar_days_fk(df=tmp, left_on=col_vacc_date)

    db.insert_or_update(df=tmp, table=table)
    db.db_close()


import os
from read_config import read_yaml
from get_data import rki, estat, divi, genesis
TODAY = dt.date.today()
TODAY = dt.datetime(TODAY.year, TODAY.month, TODAY.day)
if __name__ == '__main__':
    config = read_yaml()
    config_cols = read_yaml('config_cols.yaml')
    config_db = read_yaml('config_db.yaml')
    df = rki(
        url=config['urls']['rki_vaccination_rates'],
        purpose='RKI_VACC_RATES',
        save_file=True,
        path=os.path.join(config['paths']['root'], config['paths']['vaccinations'], '')
    )
    df = rki_vaccinations_daily(config_cols=config_cols, config_db=config_db, df=df)

