import eurostat
from src.database import db_helper as database
from src.utils import estat_helper


def population_by_states(table: str, countries: list = None, starting_year: int = 1990):
    # Datasource: Eurostat

    db = database.ProjDB()

    df = eurostat.get_data_df(
        'demo_r_d2jan',
        flags=False
    )

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

    df_subdivision_0 = df[df['level'] == 0].copy()
    df_subdivision_1 = df[df['level'] == 1].copy()
    df_subdivision_2 = df[df['level'] == 2].copy()

    df_subdivision_0 = db.merge_calendar_years_fk(df_subdivision_0, left_on='year')
    df_subdivision_1 = db.merge_calendar_years_fk(df_subdivision_1, left_on='year')
    df_subdivision_2 = db.merge_calendar_years_fk(df_subdivision_2, left_on='year')

    df_subdivision_0 = db.merge_countries_fk(df_subdivision_0, left_on='geo', country_code='nuts_0')

    df_subdivision_1 = db.merge_subdivisions_fk(df_subdivision_1, left_on='geo', subdiv_code='nuts_1', level=1)
    df_subdivision_1 = df_subdivision_1[df_subdivision_1[
        'country_subdivs_1_fk'].notna()]  # if no foreign key merged, then region is probably not available

    df_subdivision_2 = db.merge_subdivisions_fk(df_subdivision_2, left_on='geo', subdiv_code='nuts_2', level=2)
    df_subdivision_2 = df_subdivision_2[df_subdivision_2['country_subdivs_2_fk'].notna()]

    # remove not needed columns
    del df_subdivision_0['level']
    del df_subdivision_0['geo']

    del df_subdivision_1['level']
    del df_subdivision_1['geo']
    del df_subdivision_1['subdivision_1']
    del df_subdivision_1['countries_fk']
    del df_subdivision_1['iso_3166_2']

    del df_subdivision_2['level']
    del df_subdivision_2['geo']
    del df_subdivision_2['subdivision_2']
    del df_subdivision_2['country_subdivs_1_fk']

    db.insert_or_update(df=df_subdivision_0, table=table)
    db.insert_or_update(df=df_subdivision_1, table=table)
    db.insert_or_update(df=df_subdivision_2, table=table)

    db.db_close()
