import isoweek
from datetime import datetime, date
import pandas as pd


def _create_base_calendar_df(start_date: date, end_date: date) -> pd.DataFrame:
    daterange = pd.date_range(start_date, end_date)
    iso_year_to_iso_week_map = dict()
    
    # TODO: is strftime correct? causes problems with _calendar_days
    for single_date in daterange:
        iso_year = single_date.strftime("%G-%m-%d")
        iso_week = single_date.strftime("%V")
        iso_year_to_iso_week_map[iso_year] = iso_week

    # create dataframe from dict
    df = pd.DataFrame.from_dict(
        iso_year_to_iso_week_map, 
        orient="index").reset_index(level=0)
    df.columns = ["iso_day", "iso_week"]
    df.index += 1 # start index at 1

    df["iso_day"] = pd.to_datetime(df["iso_day"])

    return df


def _add_meta_cols(df: pd.DataFrame, unique_key: str) -> pd.DataFrame:
    tmp = df.copy()
    tmp["created_on"] = datetime.now()
    tmp["updated_on"] = datetime.now()
    tmp["unique_key"] = df[unique_key]
    return tmp


def create_calendar_years_df(start_year: int, end_year: int) -> pd.DataFrame:
    years = list()
    for year in range(start_year, end_year+1):
        years.append(year)

    df_years = pd.DataFrame(years, columns=["iso_year"])
    df_years.index += 1 # start index at 1

    df_years = _add_meta_cols(df_years, unique_key="iso_year")

    return df_years


def create_calendar_weeks_df(start_year: int, end_year: int) -> pd.DataFrame:
    # get a dataframe with years
    df_years = create_calendar_years_df(start_year, end_year)

    # create a map as {iso_year: index} where iso_year is the key
    df_years.reset_index(level=0, inplace=True)
    dict_list = df_years.set_index("iso_year").T.to_dict("records")
    iso_year_to_idx_map, *_ = dict_list # unpacking
    
    # create a base calendar
    start_date = date(start_year, 1, 1)
    end_date = date(end_year, 12, 31)
    df_base = _create_base_calendar_df(start_date, end_date)

    # split iso_day
    df_base["iso_year"] = pd.to_datetime(df_base["iso_day"]).dt.year
    df_base["iso_key"] = df_base["iso_year"].astype(str) + df_base["iso_week"]

    # group by iso_key
    df_weeks = df_base.groupby(
        ['iso_key'], as_index=False
    ).agg({'iso_day': 'first', 'iso_week': 'first', 'iso_year': 'first'}).copy()

    # clean up df a bit
    df_weeks.drop("iso_day", axis=1, inplace=True)
    df_weeks["iso_week"] = df_weeks["iso_week"].astype(int)
    df_weeks["iso_year"] = df_weeks["iso_year"].astype(int)
    df_weeks.index += 1 # start index at 1

    # create foreign key column
    df_weeks["calendar_years_fk"] = df_weeks["iso_year"].astype(int).map(iso_year_to_idx_map)

    # final df with correct column order
    df = df_weeks[["calendar_years_fk", "iso_week", "iso_key"]].copy()
    df = _add_meta_cols(df, unique_key="iso_key")

    return df


def create_calendar_days_df(start_year: int, end_year: int) -> pd.DataFrame:
    # get a dataframe with years
    df_years = create_calendar_years_df(start_year, end_year)

    # create base df as calendar
    start_date = date(start_year, 1, 1)
    end_date = date(end_year, 12, 31)
    df_base = _create_base_calendar_df(start_date, end_date)
    df_base.to_csv("test.csv", sep=";")

    # TODO:
    pass
