import pandas as pd

AGEGROUP_10Y_MAP = {
    'Y_LT1': '00-09', 'Y1-4': '00-09', 'Y5-9': '00-09',
    'Y10-14': '10-19', 'Y15-19': '10-19',
    'Y20-24': '20-29', 'Y25-29': '20-29',
    'Y30-34': '30-39', 'Y35-39': '30-39',
    'Y40-44': '40-49', 'Y45-49': '40-49',
    'Y50-54': '50-59', 'Y55-59': '50-59',
    'Y60-64': '60-69', 'Y65-69': '60-69',
    'Y70-74': '70-79', 'Y75-79': '70-79',
    'Y80-84': '80+', 'Y85-89': '80+', 'Y90-94': '80+', 'Y_GE95': '80+'
}


def clear_estat_data(df: pd.DataFrame):
    for i in df.columns:
        if i not in ('age', 'sex', 'unit', 'geo\\time', 'icd10', 'resid'):
            df[i] = df[i].fillna(0)
            df[i] = df[i].astype(int)
    df.rename(columns={'geo\\time': 'geo'}, inplace=True)

    return df
