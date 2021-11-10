import eurostat
from src.database import db_helper as database
from src.utils import estat_helper


def annual_death_causes(table: str, countries: list):
    # Datasource: Eurostat

    db_proj = database.ProjDB()

    df = eurostat.get_data_df(
        'hlth_cd_aro',
        flags=False
    )

    # clearing some usual eurostat stuff
    df = estat_helper.clear_estat_data(df)

    # filtering only needed data
    df = df.query(
        '''
        geo == @countries \
        & age != 'TOTAL' & age !='Y_LT15' & age != 'Y15-24' & age != 'Y_LT25' & age != 'Y_LT65' \
        & age != 'Y_GE65' & age != 'Y_GE85' \
        & sex == 'T' \
        & resid == 'TOT_IN' \
        & icd10 != 'A-R_V-Y'
        '''
    )

    # melting to years
    df = df.melt(
        id_vars=['age', 'sex', 'unit', 'geo', 'icd10', 'resid'],
        var_name='year',
        value_name='deaths'
    )

    # assign 10-year agegroups
    df = df.assign(
        agegroup_10y=df['age'].map(
            estat_helper.ANNUAL_DEATH_CAUSES_AGEGROUP_10Y_MAP
        )
    ).fillna('UNK')

    # false icd10 categories
    df.loc[df['icd10'].str.contains('K72-K75'), 'icd10'] = 'K71-K77'
    df.loc[df['icd10'].str.contains('B180-B182'), 'icd10'] = 'B171-B182'

    # merge foreign keys
    df = db_proj.merge_calendar_years_fk(df, left_on='year')
    df = db_proj.merge_classifications_icd10_fk(df, left_on='icd10')
    df = db_proj.merge_agegroups_fk(df, left_on='agegroup_10y', interval='10y')
    df = db_proj.merge_countries_fk(df, left_on='geo', country_code='iso_3166_1_alpha2')

    del df['age']
    del df['geo']
    del df['sex']
    del df['unit']
    del df['resid']

    df = df.groupby(
        [
            'classifications_icd10_fk',
            'agegroups_10y_fk',
            'countries_fk',
            'calendar_years_fk'
        ], as_index=False
    )['deaths'].sum()

    db_proj.insert_or_update(df, table)

    db_proj.db_close()
