import pandas as pd
import datetime as dt
from src.utils import db_helper as database


def rki_calc_numbers(config_cols: dict, df: pd.DataFrame, date: dt.datetime):
    tmp = df.copy()

    amount_cases = config_cols['rki']['covid']['english']['amount_cases']
    amount_deaths = config_cols['rki']['covid']['english']['amount_deaths']
    amount_recovered = config_cols['rki']['covid']['english']['amount_recovered']
    new_cases = config_cols['rki']['covid']['english']['new_cases']
    new_deaths = config_cols['rki']['covid']['english']['new_deaths']
    new_recovered = config_cols['rki']['covid']['english']['new_recovered']
    reporting_date = config_cols['rki']['covid']['english']['reporting_date']
    reference_date = config_cols['rki']['covid']['english']['reference_date']
    date_of_disease_onset = config_cols['rki']['covid']['english']['date_of_disease_onset'] # IstErkrankungsbeginn

    if new_cases in tmp.columns:
        tmp['cases'] = tmp[[amount_cases]] \
            .sum(axis=1) \
            .where(tmp[new_cases] >= 0, 0)

        tmp['cases_delta'] = tmp[[amount_cases]] \
            .sum(axis=1) \
            .where(
            (tmp[new_cases] == 1) | (tmp[new_cases] == -1),
            0
        )

        tmp['cases_7d'] = tmp[[amount_cases]] \
            .sum(axis=1) \
            .where(
            (tmp[new_cases] >= 0) & (tmp[reporting_date] > date - dt.timedelta(days=8)),
            0
        )
    else:
        tmp['cases'] = tmp[amount_cases]
        tmp['cases_delta'] = 0

        tmp['cases_7d'] = tmp[[amount_cases]] \
            .sum(axis=1) \
            .where(tmp[reporting_date] > date - dt.timedelta(days=8), 0)

    if date_of_disease_onset in tmp.columns:
        tmp['cases_7d_sympt'] = tmp[[amount_cases]] \
            .sum(axis=1) \
            .where(
            (tmp[new_cases] >= 0) &
            (tmp[reporting_date] > date - dt.timedelta(days=8)) &
            (tmp[date_of_disease_onset] == 1),
            0
        )
    else:
        tmp['cases_7d_sympt'] = tmp[['cases_7d']]

    if reference_date in tmp.columns:
        tmp['cases_delta_ref'] = tmp[[amount_cases]] \
            .sum(axis=1) \
            .where(
            ((tmp[new_cases] == 1) | (tmp[new_cases] == -1)) &
            (tmp[reference_date] > date - dt.timedelta(days=2)),
            0
        )

        tmp['cases_7d_ref'] = tmp[[amount_cases]] \
            .sum(axis=1) \
            .where(
            (tmp[new_cases] >= 0) &
            (tmp[reporting_date] > date - dt.timedelta(days=8)) &
            (tmp[reference_date] > date - dt.timedelta(days=8)),
            0
        )
    else:
        tmp['cases_delta_ref'] = tmp[['cases_delta']]
        tmp['cases_7d_ref'] = tmp[['cases_7d']]

    if reference_date in tmp.columns and date_of_disease_onset in tmp.columns:
        tmp['cases_delta_ref_sympt'] = tmp[[amount_cases]] \
            .sum(axis=1) \
            .where(
            ((tmp[new_cases] == 1) | (tmp[new_cases] == -1)) &
            (tmp[reference_date] > date - dt.timedelta(days=2)) &
            (tmp[date_of_disease_onset] == 1),
            0
        )

        tmp['cases_7d_ref_sympt'] = tmp[[amount_cases]] \
            .sum(axis=1) \
            .where(
            (tmp[new_cases] >= 0) &
            (tmp[reporting_date] > date - dt.timedelta(days=8)) &
            (tmp[reference_date] > date - dt.timedelta(days=8)) &
            (tmp[date_of_disease_onset] == 1),
            0
        )
    else:
        tmp['cases_delta_ref_sympt'] = tmp[['cases_delta']]
        tmp['cases_7d_ref_sympt'] = tmp[['cases_7d']]

    if new_deaths in tmp.columns:
        tmp['deaths'] = tmp[[amount_deaths]] \
            .sum(axis=1) \
            .where(tmp[new_deaths] >= 0, 0)

        tmp['deaths_delta'] = tmp[[amount_deaths]] \
            .sum(axis=1) \
            .where(
            (tmp[new_deaths] == 1) | (tmp[new_deaths] == -1),
            0
        )
    else:
        tmp['deaths'] = tmp[amount_deaths]
        tmp['deaths_delta'] = 0

    if new_recovered in tmp.columns:
        tmp['recovered'] = tmp[[amount_recovered]] \
            .sum(axis=1) \
            .where(tmp[new_recovered] >= 0, 0)

        tmp['recovered_delta'] = tmp[[amount_recovered]] \
            .sum(axis=1) \
            .where(
            (tmp[new_recovered] == 1) | (tmp[new_recovered] == -1),
            0
        )
    else:
        if amount_recovered in tmp.columns:
            tmp['recovered'] = tmp[amount_recovered]
            tmp['recovered_delta'] = 0
        else:
            tmp['recovered'] = 0
            tmp['recovered_delta'] = 0

    # corona active cases
    tmp['active_cases'] = tmp['cases'] - (tmp['deaths'] + tmp['recovered'])
    tmp['active_cases_delta'] = tmp['cases_delta'] - (tmp['deaths_delta'] + tmp['recovered_delta'])

    # override to today's date
    tmp['reporting_date'] = date

    # tmp = tmp[
    #     ['IdBundesland',
    #      'IdLandkreis',
    #      'rki_agegroups',
    #      'reporting_date',
    #      'cases',
    #      'cases_delta',
    #      'cases_delta_ref',
    #      'cases_delta_ref_sympt',
    #      'cases_7d',
    #      'cases_7d_sympt',
    #      'cases_7d_ref',
    #      'cases_7d_ref_sympt',
    #      'deaths',
    #      'deaths_delta',
    #      'recovered',
    #      'recovered_delta',
    #      'active_cases',
    #      'active_cases_delta'
    #      ]
    # ]

    return tmp


def rki_calc_7d_incidence(config_cols: dict, config_db: dict, df: pd.DataFrame, level: int, reference_year: str):
    tmp = df.copy()

    subdivision_1_id = config_cols['rki']['covid']['english']['subdivision_1_id']
    subdivision_2_id = config_cols['rki']['covid']['english']['subdivision_2_id']

    # create db connection
    db = database.ProjDB()

    if level == 3:
        df_population = db.get_population(country='DE', country_code='iso_3166_1_alpha2', level=3, year=reference_year)
        # merge states population
        tmp = tmp.merge(df_population,
                        left_on=subdivision_2_id,
                        right_on='ags',
                        how='left',
                        )
        tmp = tmp[tmp['country_subdivs_3_fk'].notna()]
    elif level == 1:
        df_population = db.get_population(country='DE', country_code='iso_3166_1_alpha2', level=1, year=reference_year)
        tmp = tmp.merge(df_population,
                        left_on=subdivision_1_id,
                        right_on='bundesland_id',
                        how='left',
                        )
        tmp = tmp[tmp['country_subdivs_1_fk'].notna()]
    else:
        df_population = db.get_population(country='DE', country_code='iso_3166_1_alpha2', level=0, year=reference_year)
        tmp = tmp.merge(df_population,
                        left_on='geo',
                        right_on='nuts_0',
                        how='left',
                        )

    # incidence 7 days
    tmp['incidence_7d'] = (tmp['cases_7d'] / tmp['population']) * 100000
    tmp['incidence_7d_sympt'] = (tmp['cases_7d_sympt'] / tmp['population']) * 100000
    tmp['incidence_7d_ref'] = (tmp['cases_7d_ref'] / tmp['population']) * 100000
    tmp['incidence_7d_ref_sympt'] = (tmp['cases_7d_ref_sympt'] / tmp['population']) * 100000

    db.db_close()

    return tmp


def rki_pre_process(config_cols: dict, df: pd.DataFrame):
    tmp = df.copy()

    reporting_date = config_cols['rki']['covid']['english']['reporting_date']
    reference_date = config_cols['rki']['covid']['english']['reference_date']

    if reporting_date in tmp.columns:
        try:
            tmp[reporting_date] = pd.to_datetime(tmp[reporting_date], infer_datetime_format=True).dt.date
            tmp[reporting_date] = pd.to_datetime(tmp[reporting_date], infer_datetime_format=True)
        except (KeyError, TypeError):
            print('Error trying to convert Date columns')

    if reference_date in tmp.columns:
        try:
            tmp[reference_date] = pd.to_datetime(tmp[reference_date], infer_datetime_format=True).dt.date
            tmp[reference_date] = pd.to_datetime(tmp[reference_date], infer_datetime_format=True)
        except (KeyError, TypeError):
            print('Error trying to convert Date columns')

    # remove whitespaces from header
    tmp.columns = tmp.columns.str.replace(' ', '')

    return tmp


def pre_process_tests(df: pd.DataFrame):
    tmp = df.copy()
    tmp.columns = ['calendar_week', 'amount', 'positive', 'positive_percentage', 'amount_transferring_laboratories']
    tmp = tmp[1:]  # delete first row
    tmp = tmp[:-1]  # delete last row
    # create ISO dates
    tmp[['iso_cw', 'iso_year']] = tmp['calendar_week'].str.split('/', expand=True)
    tmp['iso_cw'] = tmp['iso_cw'].str.zfill(2)
    tmp['iso_key'] = tmp['iso_year'] + tmp['iso_cw']
    tmp['iso_key'] = pd.to_numeric(tmp['iso_key'], errors='coerce')

    return tmp


def pre_process_rvalue(df: pd.DataFrame):
    tmp = df.copy()
    tmp = tmp.fillna(0)
    tmp['Datum'] = pd.to_datetime(tmp['Datum'])
    tmp.rename(
        columns={
            'Datum': 'date',
            'PS_COVID_Faelle': 'point_estimation_covid',
            'UG_PI_COVID_Faelle': 'll_prediction_interval_covid',
            'OG_PI_COVID_Faelle': 'ul_prediction_interval_covid',
            'PS_COVID_Faelle_ma4': 'point_estimation_covid_smoothed',
            'UG_PI_COVID_Faelle_ma4': 'll_prediction_interval_covid_smoothed',
            'OG_PI_COVID_Faelle_ma4': 'ul_prediction_interval_covid_smoothed',
            'PS_7_Tage_R_Wert': 'point_estimation_7_day_rvalue',
            'UG_PI_7_Tage_R_Wert': 'll_prediction_interval_7_day_rvalue',
            'OG_PI_7_Tage_R_Wert': 'ul_prediction_interval_7_day_rvalue'

        },
        inplace=True
    )

    return tmp


def pre_process_vaccination_states(df: pd.DataFrame):
    tmp = df.copy()
    tmp = tmp[tmp['Impfstoff'].notna()]
    if 'Impfdatum' in tmp.columns:
        try:
            tmp['Impfdatum'] = pd.to_datetime(tmp['Impfdatum'], infer_datetime_format=True).dt.date
            tmp['Impfdatum'] = pd.to_datetime(tmp['Impfdatum'], infer_datetime_format=True)
        except (KeyError, TypeError):
            print('Error trying to convert Date columns')

    tmp.rename(
        columns={
            'Anzahl': 'amount'
        },
        inplace=True
    )

    return tmp
