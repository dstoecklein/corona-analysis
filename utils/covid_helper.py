import datetime as dt

import pandas as pd

import utils.db_helper as database
from config.core import config, config_db

# Constants
AMOUNT_CASES = config.cols.rki_covid_daily["cols"]["amount_cases"]
AMOUNT_DEATHS = config.cols.rki_covid_daily["cols"]['amount_deaths']
AMOUNT_RECOVERED = config.cols.rki_covid_daily["cols"]['amount_recovered']
NEW_CASES = config.cols.rki_covid_daily["cols"]['new_cases']
NEW_DEATHS = config.cols.rki_covid_daily["cols"]['new_deaths']
NEW_RECOVERED = config.cols.rki_covid_daily["cols"]['new_recovered']
REPORTING_DATE = config.cols.rki_covid_daily["cols"]['reporting_date']
REFERENCE_DATE = config.cols.rki_covid_daily["cols"]['reference_date']
DATE_OF_DISEASE_ONSET = config.cols.rki_covid_daily["cols"]['date_of_disease_onset'] # IstErkrankungsbeginn
SUBDIVISION_1 = config.cols.rki_covid_daily['cols']['subdivision_1']
SUBDIVISION_2 = config.cols.rki_covid_daily['cols']['subdivision_2']
SUBDIVISION_2_ID = config.cols.rki_covid_daily['cols']['subdivision_2_id']
NUTS_0 = config_db.cols['_countries']['nuts_0']
ISO_3166_1_ALPHA2 = config_db.cols['_countries']['iso_3166_1_alpha2']
AGS = config_db.cols['_country_subdivs_3']['ags']
BUNDESLAND_ID = config_db.cols['_country_subdivs_1']['bundesland_id']

def rki_calc_numbers(df: pd.DataFrame, date: dt.datetime) -> pd.DataFrame:
    tmp = df.copy()

    if NEW_CASES in tmp.columns:
        tmp['cases'] = tmp[[AMOUNT_CASES]] \
            .sum(axis=1) \
            .where(tmp[NEW_CASES] >= 0, 0)

        tmp['cases_delta'] = tmp[[AMOUNT_CASES]] \
            .sum(axis=1) \
            .where(
            (tmp[NEW_CASES] == 1) | (tmp[NEW_CASES] == -1),
            0
        )

        tmp['cases_7d'] = tmp[[AMOUNT_CASES]] \
            .sum(axis=1) \
            .where(
            (tmp[NEW_CASES] >= 0) & (tmp[REPORTING_DATE] > date - dt.timedelta(days=8)),
            0
        )
    else:
        tmp['cases'] = tmp[AMOUNT_CASES]
        tmp['cases_delta'] = 0

        tmp['cases_7d'] = tmp[[AMOUNT_CASES]] \
            .sum(axis=1) \
            .where(tmp[REPORTING_DATE] > date - dt.timedelta(days=8), 0)

    if DATE_OF_DISEASE_ONSET in tmp.columns:
        tmp['cases_7d_sympt'] = tmp[[AMOUNT_CASES]] \
            .sum(axis=1) \
            .where(
            (tmp[NEW_CASES] >= 0) &
            (tmp[REPORTING_DATE] > date - dt.timedelta(days=8)) &
            (tmp[DATE_OF_DISEASE_ONSET] == 1),
            0
        )
    else:
        tmp['cases_7d_sympt'] = tmp[['cases_7d']]

    if REFERENCE_DATE in tmp.columns:
        tmp['cases_delta_ref'] = tmp[[AMOUNT_CASES]] \
            .sum(axis=1) \
            .where(
            ((tmp[NEW_CASES] == 1) | (tmp[NEW_CASES] == -1)) &
            (tmp[REFERENCE_DATE] > date - dt.timedelta(days=2)),
            0
        )

        tmp['cases_7d_ref'] = tmp[[AMOUNT_CASES]] \
            .sum(axis=1) \
            .where(
            (tmp[NEW_CASES] >= 0) &
            (tmp[REPORTING_DATE] > date - dt.timedelta(days=8)) &
            (tmp[REFERENCE_DATE] > date - dt.timedelta(days=8)),
            0
        )
    else:
        tmp['cases_delta_ref'] = tmp[['cases_delta']]
        tmp['cases_7d_ref'] = tmp[['cases_7d']]

    if REFERENCE_DATE in tmp.columns and DATE_OF_DISEASE_ONSET in tmp.columns:
        tmp['cases_delta_ref_sympt'] = tmp[[AMOUNT_CASES]] \
            .sum(axis=1) \
            .where(
            ((tmp[NEW_CASES] == 1) | (tmp[NEW_CASES] == -1)) &
            (tmp[REFERENCE_DATE] > date - dt.timedelta(days=2)) &
            (tmp[DATE_OF_DISEASE_ONSET] == 1),
            0
        )

        tmp['cases_7d_ref_sympt'] = tmp[[AMOUNT_CASES]] \
            .sum(axis=1) \
            .where(
            (tmp[NEW_CASES] >= 0) &
            (tmp[REPORTING_DATE] > date - dt.timedelta(days=8)) &
            (tmp[REFERENCE_DATE] > date - dt.timedelta(days=8)) &
            (tmp[DATE_OF_DISEASE_ONSET] == 1),
            0
        )
    else:
        tmp['cases_delta_ref_sympt'] = tmp[['cases_delta']]
        tmp['cases_7d_ref_sympt'] = tmp[['cases_7d']]

    if NEW_DEATHS in tmp.columns:
        tmp['deaths'] = tmp[[AMOUNT_DEATHS]] \
            .sum(axis=1) \
            .where(tmp[NEW_DEATHS] >= 0, 0)

        tmp['deaths_delta'] = tmp[[AMOUNT_DEATHS]] \
            .sum(axis=1) \
            .where(
            (tmp[NEW_DEATHS] == 1) | (tmp[NEW_DEATHS] == -1),
            0
        )
    else:
        tmp['deaths'] = tmp[AMOUNT_DEATHS]
        tmp['deaths_delta'] = 0

    if NEW_RECOVERED in tmp.columns:
        tmp['recovered'] = tmp[[AMOUNT_RECOVERED]] \
            .sum(axis=1) \
            .where(tmp[NEW_RECOVERED] >= 0, 0)

        tmp['recovered_delta'] = tmp[[AMOUNT_RECOVERED]] \
            .sum(axis=1) \
            .where(
            (tmp[NEW_RECOVERED] == 1) | (tmp[NEW_RECOVERED] == -1),
            0
        )
    else:
        if AMOUNT_RECOVERED in tmp.columns:
            tmp['recovered'] = tmp[AMOUNT_RECOVERED]
            tmp['recovered_delta'] = 0
        else:
            tmp['recovered'] = 0
            tmp['recovered_delta'] = 0

    # corona active cases
    tmp['active_cases'] = tmp['cases'] - (tmp['deaths'] + tmp['recovered'])
    tmp['active_cases_delta'] = tmp['cases_delta'] - (tmp['deaths_delta'] + tmp['recovered_delta'])

    # override to today's date
    tmp['reporting_date'] = date

    return tmp


def rki_calc_7d_incidence(df: pd.DataFrame, level: int, reference_year: str) -> pd.DataFrame:
    tmp = df.copy()

    # create db connection
    db = database.ProjDB()

    if level == 3:
        df_population = db.get_population(country='DE', country_code=ISO_3166_1_ALPHA2, level=3, year=reference_year)
        # merge states population
        tmp = tmp.merge(df_population,
                        left_on=SUBDIVISION_2_ID,
                        right_on=AGS,
                        how='left',
                        )
        tmp = tmp[tmp['country_subdivs_3_fk'].notna()]
    elif level == 1:
        df_population = db.get_population(country='DE', country_code=ISO_3166_1_ALPHA2, level=1, year=reference_year)
        tmp = tmp.merge(df_population,
                        left_on=BUNDESLAND_ID,
                        right_on=BUNDESLAND_ID,
                        how='left',
                        )
        tmp = tmp[tmp['country_subdivs_1_fk'].notna()]
    else:
        df_population = db.get_population(country='DE', country_code=ISO_3166_1_ALPHA2, level=0, year=reference_year)
        tmp = tmp.merge(df_population,
                        left_on='geo',
                        right_on=NUTS_0,
                        how='left',
                        )

    # incidence 7 days
    tmp['incidence_7d'] = (tmp['cases_7d'] / tmp['population']) * 100000
    tmp['incidence_7d_sympt'] = (tmp['cases_7d_sympt'] / tmp['population']) * 100000
    tmp['incidence_7d_ref'] = (tmp['cases_7d_ref'] / tmp['population']) * 100000
    tmp['incidence_7d_ref_sympt'] = (tmp['cases_7d_ref_sympt'] / tmp['population']) * 100000

    db.db_close()

    return tmp


def rki_pre_process(df: pd.DataFrame) -> pd.DataFrame:
    tmp = df.copy()

    if REPORTING_DATE in tmp.columns:
        try:
            tmp[REPORTING_DATE] = pd.to_datetime(tmp[REPORTING_DATE], infer_datetime_format=True).dt.date
            tmp[REPORTING_DATE] = pd.to_datetime(tmp[REPORTING_DATE], infer_datetime_format=True)
        except (KeyError, TypeError):
            print('Error trying to convert Date columns')

    if REFERENCE_DATE in tmp.columns:
        try:
            tmp[REFERENCE_DATE] = pd.to_datetime(tmp[REFERENCE_DATE], infer_datetime_format=True).dt.date
            tmp[REFERENCE_DATE] = pd.to_datetime(tmp[REFERENCE_DATE], infer_datetime_format=True)
        except (KeyError, TypeError):
            print('Error trying to convert Date columns')

    # remove whitespaces from header
    tmp.columns = tmp.columns.str.replace(' ', '')

    return tmp


def pre_process_tests(df: pd.DataFrame) -> pd.DataFrame:
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


def pre_process_rvalue(df: pd.DataFrame) -> pd.DataFrame:
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


def pre_process_vaccination_states(df: pd.DataFrame) -> pd.DataFrame:
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
