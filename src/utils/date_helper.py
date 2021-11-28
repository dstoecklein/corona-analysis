import pandas as pd


def create_iso_key(df: pd.DataFrame, column_name: str):
    tmp = df.copy()
    tmp = tmp.assign(iso_key=tmp[column_name].dt.strftime('%G%V').astype(int))
    return tmp


def create_calendar_years(df: pd.DataFrame):
    tmp = df.copy()
    tmp = tmp.drop_duplicates(subset=['iso_year'])
    tmp = tmp.reset_index(drop=True)
    tmp['calendar_years_fk'] = tmp.index + 1
    del tmp['iso_day']
    del tmp['iso_week']
    del tmp['iso_key']
    tmp.to_csv('years.csv', sep=";", index=False)
    return tmp


def create_calendar_weeks(df: pd.DataFrame):
    tmp = df.copy()
    tmp = tmp.drop_duplicates(subset=['iso_key'])

    df_years = create_calendar_years(tmp)
    tmp = tmp.merge(df_years,
                    left_on='iso_year',
                    right_on='iso_year',
                    how='left'
                    )

    del tmp['iso_day']
    del tmp['iso_year']

    tmp = tmp.reset_index(drop=True)
    tmp['calendar_weeks_fk'] = tmp.index + 1

    tmp.to_csv('weeks.csv', sep=";", index=False)
    return tmp


def create_calendar_day(df: pd.DataFrame):
    tmp = df.copy()
    df_weeks = create_calendar_weeks(tmp)

    tmp = tmp.merge(df_weeks,
                    left_on='iso_key',
                    right_on='iso_key',
                    how='left'
                    ).drop(['calendar_years_fk', 'iso_week_x', 'iso_year', 'iso_key', 'iso_week_y'], axis=1)

    tmp.to_csv("days.csv", sep=";", index=False)
    return tmp

# df = pd.read_csv("days_new.csv", sep=";")
# df['iso_day'] = pd.to_datetime(df['iso_day'], infer_datetime_format=True).dt.date
# df['iso_day'] = pd.to_datetime(df['iso_day'], infer_datetime_format=True)
# df['iso_week'] = df['iso_day'].apply(lambda x: x.isocalendar()[1]).astype(str).str.zfill(2)
# df['iso_year'] = df['iso_day'].apply(lambda x: x.isocalendar()[0]).astype(str)
# df['iso_key'] = df['iso_year'] + df['iso_week']
# df['iso_year'] = pd.to_numeric(df['iso_year'], errors='coerce')
# df['iso_key'] = pd.to_numeric(df['iso_key'], errors='coerce')
# create_calendar_years(df=df)
# create_calendar_weeks(df=df)
# create_calendar_day(df=df)
