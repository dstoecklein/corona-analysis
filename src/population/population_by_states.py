import eurostat
import pandas as pd
from src.database import db_helper as database
from src.utils import estat_helper


def population_by_states(table: str, countries: list, starting_year: int = 1990):
    # Datasource: Eurostat

    db = database.ProjDB()

    # df = eurostat.get_data_df(
    #    'demo_r_d2jan',
    #    flags=False
    # )

    # df.to_csv("test.csv", index=False, sep=";")

    df = pd.read_csv("test.csv", sep=";", encoding="utf8")
    df = estat_helper.clear_estat_data(df)

    # if only certain countries, otherwise all
    if countries:
        bool_series = df['geo'].str.contains('|'.join(countries))
        df = df[bool_series]

    # only need totals
    df.query(
        '''
        age == 'TOTAL' \
        & sex == 'T' 
        ''',
        inplace=True
    )

    # new column 'level' to indicate NUTS-level
    df = df.assign(level=df['geo'].str.len() - 2)

    # filter to NUTS-3
    df = df[df['level'].astype(int) <= 3]

    df.drop(['unit', 'sex', 'age'], axis=1, inplace=True)

    df = df.melt(
        id_vars=['geo', 'level'],
        var_name='year',
        value_name='population'
    )

    # selection from year
    df = df[df['year'].astype(int) >= starting_year]

    df_nuts0 = df[df['level'] == 0].copy()
    df_nuts1 = df[df['level'] == 1].copy()
    df_nuts2 = df[df['level'] == 2].copy()

    df_nuts0 = db.merge_calendar_years_fk(df_nuts0, left_on='year')
    df_nuts0 = db.merge_countries_fk(df_nuts0, left_on='geo', country_code='nuts_0')

    # remove not needed columns
    del df['level']
    del df['geo']

    df_nuts0.to_csv("nuts0.csv", sep=";", index=False)
    # print(df.head())
    db.db_close()
    # clearing some usual eurostat stuff


population_by_states("test", [], 2010)
