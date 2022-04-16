import pandas as pd

ANNUAL_POPULATION_AGEGROUP_10Y_MAP = {
    'Y_LT5': '00-09', 'Y5-9': '00-09',
    'Y10-14': '10-19', 'Y15-19': '10-19',
    'Y20-24': '20-29', 'Y25-29': '20-29',
    'Y30-34': '30-39', 'Y35-39': '30-39',
    'Y40-44': '40-49', 'Y45-49': '40-49',
    'Y50-54': '50-59', 'Y55-59': '50-59',
    'Y60-64': '60-69', 'Y65-69': '60-69',
    'Y70-74': '70-79', 'Y75-79': '70-79',
    'Y_GE80': '80+'
}




def pre_process_float(df: pd.DataFrame):
    tmp = df.copy()
    for i in tmp.columns:
        if i not in ('age', 'sex', 'unit', 'geo', 'geo\\time', 'icd10', 'resid', 'indic_de'):
            tmp[i] = tmp[i].fillna(0)
            tmp[i] = tmp[i].astype(float)
    tmp.rename(columns={'geo\\time': 'geo'}, inplace=True)

    return tmp


def pre_process_population_states(df: pd.DataFrame):
    tmp = df.copy()
    tmp = pre_process(tmp)

    # only need totals
    tmp.query(
        '''
        age == 'TOTAL' \
        & sex == 'T' 
        ''',
        inplace=True
    )

    # new column 'level' to indicate NUTS-level
    tmp = tmp.assign(level=tmp['geo'].str.len() - 2)

    # filter to NUTS-3
    tmp = tmp[tmp['level'].astype(int) <= 3]

    tmp.drop(['unit', 'sex', 'age'], axis=1, inplace=True)

    tmp = tmp.melt(
        id_vars=['geo', 'level'],
        var_name='year',
        value_name='population'
    )

    return tmp


def pre_process_population_agegroups(df: pd.DataFrame):
    tmp = df.copy()
    tmp = pre_process(tmp)

    tmp = tmp.query(
        '''
        geo.str.len() == 2 \
        & age != 'TOTAL' & age !='Y_GE75' & age != 'Y80-84' & age != 'Y_GE85' \
        & sex == 'T' 
        '''
    )

    # melting to years
    tmp = tmp.melt(
        id_vars=['age', 'sex', 'unit', 'geo'],
        var_name='year',
        value_name='population'
    )

    # assign 10-year agegroups
    tmp = tmp.assign(
        agegroup_10y=tmp['age'].map(
            ANNUAL_POPULATION_AGEGROUP_10Y_MAP
        )
    ).fillna('UNK')

    tmp['year'] = pd.to_numeric(tmp['year'], errors='coerce')
    # selection from year
    tmp = tmp[tmp['year'] >= 1990]

    tmp = tmp.groupby(
        [
            'geo',
            'year',
            'agegroup_10y'
        ], as_index=False
    )['population'].sum()

    return tmp




def pre_process_life_exp_at_birth(df: pd.DataFrame):
    tmp = df.copy()

    tmp = pre_process_float(tmp)

    tmp.query(
        '''
        geo.str.len() == 2 \
        & age =='Y_LT1' \
        & sex == 'T'
        ''',
        inplace=True
    )

    tmp = tmp.melt(
        id_vars=['age', 'sex', 'unit', 'geo'],
        var_name='year',
        value_name='life_expectancy'
    )

    tmp = tmp[tmp['year'] >= 1990]

    return tmp


def pre_process_median_age(df: pd.DataFrame):
    tmp = df.copy()

    tmp = pre_process_float(tmp)

    tmp.query(
        '''
        indic_de == 'MEDAGEPOP' \
        & geo.str.len() == 2
        ''',
        inplace=True
    )

    tmp = tmp.melt(
        id_vars=['indic_de', 'geo'],
        var_name='year',
        value_name='median_age'
    )

    tmp = tmp[tmp['year'] >= 1990]

    return tmp
