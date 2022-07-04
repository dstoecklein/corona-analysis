import pandas as pd
from datetime import date, datetime
from db_helper2 import Database
from config.core2 import cfg_table_names


DB = Database()

def _create_base_calendar_df(start_year: int, end_year: int) -> pd.DataFrame:
    start_date = date(start_year, 1, 1)
    end_date = date(end_year, 12, 31)
    date_range = pd.date_range(start_date, end_date)

    calendar_dict = dict()

    for single_date in date_range:
        iso_values_list = list()
        regular_day = single_date.strftime("%Y-%m-%d")
        iso_week = single_date.isocalendar().week
        iso_year = single_date.isocalendar().year

        # fill list with iso values 
        iso_values_list.append(iso_week)
        iso_values_list.append(iso_year)

        # fill calendar dict {regular_day: iso_values}
        calendar_dict[regular_day] = iso_values_list

    # create dataframe
    df_base = pd.DataFrame.from_dict(calendar_dict, orient="index").reset_index(level=0)
    df_base.columns = ["iso_day", "iso_week", "iso_year"]
    df_base["iso_week"] = df_base["iso_week"].astype(str).str.zfill(2)
    df_base["iso_key"] = df_base["iso_year"].astype(str) + df_base["iso_week"].astype(str)
    df_base.index += 1 # start index at 1

    return df_base


def _create_calendar_years_df(start_year: int, end_year: int) -> pd.DataFrame:
    df_base = _create_base_calendar_df(start_year, end_year)
    df_years = df_base.groupby(
        ["iso_year"], as_index=False
    ).agg({"iso_day": "first", "iso_week": "first", "iso_key": "first"}).copy()
    df_years.index = df_years.index.set_names(["ID"])
    df_years.index += 1
    df_years = df_years.reset_index().rename(
        columns={df_years.index.name: "calendar_years_id"}
    )
    df_years = df_years.iloc[:, :2] # only first two cols

    df_years["unique_key"] = df_years["iso_year"]
    df_years["created_on"] = datetime.now()
    df_years["updated_on"] = datetime.now()

    return df_years


def _create_calendar_weeks_df(start_year: int, end_year: int) -> pd.DataFrame:
    df_base = _create_base_calendar_df(start_year, end_year)
    df_years = _create_calendar_years_df(start_year, end_year)

    df_weeks = df_base.groupby(
       ["iso_key"], as_index=False 
    ).agg({"iso_day": "first", "iso_week": "first", "iso_year": "first"}).copy()

    # create foreign key column to calendar_years_id
    df_weeks = df_weeks.merge(df_years, how="left", left_on="iso_year", right_on="iso_year")
    df_weeks.index = df_weeks.index.set_names(["ID"])
    df_weeks.index += 1
    df_weeks = df_weeks.reset_index().rename(columns={df_weeks.index.name: "calendar_weeks_id"})
    df_weeks.rename(columns={"calendar_years_id": "calendar_years_fk"}, inplace=True)
    df_weeks["iso_week"] = df_weeks["iso_week"].astype(int)
    df_weeks.drop(["iso_day", "iso_year"], axis=1, inplace=True)

    df_weeks["unique_key"] = df_weeks["iso_key"]
    df_weeks["created_on"] = datetime.now()
    df_weeks["updated_on"] = datetime.now()

    return df_weeks


def _create_calendar_days_df(start_year: int, end_year: int) -> pd.DataFrame:
    df_base = _create_base_calendar_df(start_year, end_year)
    df_weeks = _create_calendar_weeks_df(start_year, end_year)

    # create foreign key column to calendar_weeks_id
    df_day = df_base.merge(df_weeks, how="left", left_on="iso_key", right_on="iso_key", suffixes=('', '_y'))
    df_day.drop(df_day.filter(regex="_y$").columns, axis=1, inplace=True)
    df_day.drop(["iso_year", "iso_week", "iso_key", "calendar_years_fk"], axis=1, inplace=True)
    df_day.rename(columns={"calendar_weeks_id": "calendar_weeks_fk"}, inplace=True)

    df_day["unique_key"] = df_day["iso_day"]
    df_day["created_on"] = datetime.now()
    df_day["updated_on"] = datetime.now()

    return df_day


def fill_calendars(start_year: int, end_year: int) -> None:
    df_years = _create_calendar_years_df(start_year, end_year)
    df_weeks = _create_calendar_weeks_df(start_year, end_year)
    df_days = _create_calendar_days_df(start_year, end_year)

    DB.upsert_df(df=df_years, table_name=cfg_table_names.calendar_years)
    DB.upsert_df(df=df_weeks, table_name=cfg_table_names.calendar_weeks)
    DB.upsert_df(df=df_days, table_name=cfg_table_names.calendar_days)

if __name__ == "__main__":
    fill_calendars(1990, 2050)