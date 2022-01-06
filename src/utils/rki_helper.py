import pandas as pd
import datetime as dt
from src.utils import db_helper as database


def covid_calc_numbers(df: pd.DataFrame, date: dt.datetime):
    tmp = df.copy()

    if 'NeuerFall' in tmp.columns:
        tmp['cases'] = tmp[['AnzahlFall']] \
            .sum(axis=1) \
            .where(tmp['NeuerFall'] >= 0, 0)

        tmp['cases_delta'] = tmp[['AnzahlFall']] \
            .sum(axis=1) \
            .where(
            (tmp['NeuerFall'] == 1) | (tmp['NeuerFall'] == -1),
            0
        )

        tmp['cases_7d'] = tmp[['AnzahlFall']] \
            .sum(axis=1) \
            .where(
            (tmp['NeuerFall'] >= 0) & (tmp['Meldedatum'] > date - dt.timedelta(days=8)),
            0
        )
    else:
        tmp['cases'] = tmp['AnzahlFall']
        tmp['cases_delta'] = 0

        tmp['cases_7d'] = tmp[['AnzahlFall']] \
            .sum(axis=1) \
            .where(tmp['Meldedatum'] > date - dt.timedelta(days=8), 0)

    if 'IstErkrankungsbeginn' in tmp.columns:
        tmp['cases_7d_sympt'] = tmp[['AnzahlFall']] \
            .sum(axis=1) \
            .where(
            (tmp['NeuerFall'] >= 0) &
            (tmp['Meldedatum'] > date - dt.timedelta(days=8)) &
            (tmp['IstErkrankungsbeginn'] == 1),
            0
        )
    else:
        tmp['cases_7d_sympt'] = tmp[['cases_7d']]

    if 'Refdatum' in tmp.columns:
        tmp['cases_delta_ref'] = tmp[['AnzahlFall']] \
            .sum(axis=1) \
            .where(
            ((tmp['NeuerFall'] == 1) | (tmp['NeuerFall'] == -1)) &
            (tmp['Refdatum'] > date - dt.timedelta(days=2)),
            0
        )

        tmp['cases_7d_ref'] = tmp[['AnzahlFall']] \
            .sum(axis=1) \
            .where(
            (tmp['NeuerFall'] >= 0) &
            (tmp['Meldedatum'] > date - dt.timedelta(days=8)) &
            (tmp['Refdatum'] > date - dt.timedelta(days=8)),
            0
        )
    else:
        tmp['cases_delta_ref'] = tmp[['cases_delta']]
        tmp['cases_7d_ref'] = tmp[['cases_7d']]

    if 'Refdatum' in tmp.columns and 'IstErkrankungsbeginn' in tmp.columns:
        tmp['cases_delta_ref_sympt'] = tmp[['AnzahlFall']] \
            .sum(axis=1) \
            .where(
            ((tmp['NeuerFall'] == 1) | (tmp['NeuerFall'] == -1)) &
            (tmp['Refdatum'] > date - dt.timedelta(days=2)) &
            (tmp['IstErkrankungsbeginn'] == 1),
            0
        )

        tmp['cases_7d_ref_sympt'] = tmp[['AnzahlFall']] \
            .sum(axis=1) \
            .where(
            (tmp['NeuerFall'] >= 0) &
            (tmp['Meldedatum'] > date - dt.timedelta(days=8)) &
            (tmp['Refdatum'] > date - dt.timedelta(days=8)) &
            (tmp['IstErkrankungsbeginn'] == 1),
            0
        )
    else:
        tmp['cases_delta_ref_sympt'] = tmp[['cases_delta']]
        tmp['cases_7d_ref_sympt'] = tmp[['cases_7d']]

    if 'NeuerTodesfall' in tmp.columns:
        tmp['deaths'] = tmp[['AnzahlTodesfall']] \
            .sum(axis=1) \
            .where(tmp['NeuerTodesfall'] >= 0, 0)

        tmp['deaths_delta'] = tmp[['AnzahlTodesfall']] \
            .sum(axis=1) \
            .where(
            (tmp['NeuerTodesfall'] == 1) | (tmp['NeuerTodesfall'] == -1),
            0
        )
    else:
        tmp['deaths'] = tmp['AnzahlTodesfall']
        tmp['deaths_delta'] = 0

    if 'NeuGenesen' in tmp.columns:
        tmp['recovered'] = tmp[['AnzahlGenesen']] \
            .sum(axis=1) \
            .where(tmp['NeuGenesen'] >= 0, 0)

        tmp['recovered_delta'] = tmp[['AnzahlGenesen']] \
            .sum(axis=1) \
            .where(
            (tmp['NeuGenesen'] == 1) | (tmp['NeuGenesen'] == -1),
            0
        )
    else:
        if 'AnzahlGenesen' in tmp.columns:
            tmp['recovered'] = tmp['AnzahlGenesen']
            tmp['recovered_delta'] = 0
        else:
            tmp['recovered'] = 0
            tmp['recovered_delta'] = 0

    # corona active cases
    tmp['active_cases'] = tmp['cases'] - (tmp['deaths'] + tmp['recovered'])
    tmp['active_cases_delta'] = tmp['cases_delta'] - (tmp['deaths_delta'] + tmp['recovered_delta'])

    tmp['reporting_date'] = date

    tmp.rename(
        columns={
            'Altersgruppe': 'rki_agegroups'
        },
        inplace=True
    )

    tmp = tmp[
        ['IdBundesland',
         'IdLandkreis',
         'rki_agegroups',
         'reporting_date',
         'cases',
         'cases_delta',
         'cases_delta_ref',
         'cases_delta_ref_sympt',
         'cases_7d',
         'cases_7d_sympt',
         'cases_7d_ref',
         'cases_7d_ref_sympt',
         'deaths',
         'deaths_delta',
         'recovered',
         'recovered_delta',
         'active_cases',
         'active_cases_delta'
         ]
    ]

    return tmp


def calc_7d_incidence(df: pd.DataFrame, level: int, reference_year: str):
    tmp = df.copy()

    # create db connection
    db = database.ProjDB()

    if level == 3:
        df_population = db.get_population(country='DE', country_code='iso_3166_1_alpha2', level=3, year=reference_year)
        # merge states population
        tmp = tmp.merge(df_population,
                        left_on='IdLandkreis',
                        right_on='ags',
                        how='left',
                        )
        tmp = tmp[tmp['country_subdivs_3_fk'].notna()]
    elif level == 1:
        df_population = db.get_population(country='DE', country_code='iso_3166_1_alpha2', level=1, year=reference_year)
        tmp = tmp.merge(df_population,
                        left_on='IdBundesland',
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


def pre_process_covid(df: pd.DataFrame):
    tmp = df.copy()

    if 'Meldedatum' in tmp.columns:
        try:
            tmp['Meldedatum'] = pd.to_datetime(tmp['Meldedatum'], infer_datetime_format=True).dt.date
            tmp['Meldedatum'] = pd.to_datetime(tmp['Meldedatum'], infer_datetime_format=True)
        except (KeyError, TypeError):
            print('Error trying to convert Date columns')

    if 'Refdatum' in tmp.columns:
        try:
            tmp['Refdatum'] = pd.to_datetime(tmp['Refdatum'], infer_datetime_format=True).dt.date
            tmp['Refdatum'] = pd.to_datetime(tmp['Refdatum'], infer_datetime_format=True)
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
