import argparse
import datetime as dt
from config import core
from config.core import config
from utils.get_data import rki, estat, divi, genesis
from utils.csv_bulk import rki_bulk, divi_bulk
from src import (
    covid, covid_rvalue, covid_tests, covid_vaccinations, 
    intensive_care_units, mortalities, population, hospital)


TODAY = dt.date.today()
TODAY = dt.datetime(TODAY.year, TODAY.month, TODAY.day)

# Constants
COVID_FILES_PATH = core.FILES_PATH / 'covid'
COVID_TEST_FILES_PATH = core.FILES_PATH / 'covid_tests'
COVID_RVALUE_FILES_PATH = core.FILES_PATH / 'covid_rvalue'
COVID_VACC_FILES_PATH = core.FILES_PATH / 'covid_vaccinations'
ITCU_FILES_PATH = core.FILES_PATH / 'itcus'
MORTALITIES_PATH = core.FILES_PATH / 'mortalities'
HOSPITALS_PATH = core.FILES_PATH / 'hospitals'


def daily():
    """
    df_rki_covid_daily = rki(
        url=config.data.urls['rki_covid_daily'],
        purpose='RKI_COVID19_DAILY',
        save_file=True,
        path=COVID_FILES_PATH
    )
    df_rki_rvalue_daily = rki(
        url=config.data.urls['rki_rvalue_daily'],
        purpose='RKI_RVALUE_DAILY',
        save_file=True,
        path=COVID_FILES_PATH
    )
    df_rki_vacc_daily_cumulative = rki(
        url=config.data.urls['rki_vaccinations_daily_cumulative'],
        purpose='RKI_VACC_DAILY_CUMULATIVE',
        save_file=True,
        path=COVID_VACC_FILES_PATH
    )
    df_rki_vacc_daily_states = rki(
        url=config.data.urls['rki_vaccination_states'],
        purpose='RKI_VACC_DAILY_STATES',
        save_file=True,
        path=COVID_VACC_FILES_PATH
    )
    df_divi_itcu_daily_counties = divi(
        url=config.data.urls['divi_itcu_daily_counties'],
        purpose='DIVI_ITCU_DAILY_COUNTIES',
        save_file=True,
        path=ITCU_FILES_PATH
    )
    """
    df_divi_itcu_daily_states = divi(
        url=config.data.urls['divi_itcu_daily_states'],
        purpose='DIVI_ITCU_DAILY_STATES',
        save_file=True,
        path=ITCU_FILES_PATH
    ) 
    """
    covid.rki_daily(df=df_rki_covid_daily)
    covid.rki_daily_states(df=df_rki_covid_daily)
    covid.rki_daily_counties(df=df_rki_covid_daily)
    covid.rki_daily_agegroups(df=df_rki_covid_daily)
    covid.rki_weekly_cumulative(df=df_rki_covid_daily)
    covid_rvalue.rki_daily(df=df_rki_rvalue_daily)
    covid_vaccinations.rki_vaccinations_daily_cumulative(df=df_rki_vacc_daily_cumulative)
    covid_vaccinations.rki_vaccinations_daily_states(df=df_rki_vacc_daily_states)
    intensive_care_units.divi_daily_counties(df=df_divi_itcu_daily_counties)
    """
    intensive_care_units.divi_daily_states(df=df_divi_itcu_daily_states)

def weekly():
    df_rki_tests_weekly = rki(
        url=config.data.urls['rki_tests_weekly'],
        purpose='RKI_TESTS_WEEKLY',
        save_file=True,
        path=COVID_TEST_FILES_PATH,
        is_excel=True,
        sheet_name='1_Testzahlerfassung'
    )
    df_estat_deaths_weekly_agegroups = estat(
        code=config.data.estat_tables['estsat_weekly_deaths_agegroups'],
        purpose='ESTAT_DEATHS_WEEKLY_AGEGROUPS',
        save_file=True,
        path=MORTALITIES_PATH
    )

    covid_tests.rki_weekly(df=df_rki_tests_weekly)
    mortalities.estat_deaths_weekly_agegroups(df=df_estat_deaths_weekly_agegroups)


def annual():
    df_estat_death_causes_annual_agegroups = estat(
        code=config.data.estat_tables['estsat_death_causes_annual_agegroups'],
        purpose='ESTAT_DEATH_CAUSES_ANNUAL_AGEGROUPS',
        save_file=True,
        path=MORTALITIES_PATH
    )
    df_estat_population_countries = estat(
        code=config.data.estat_tables['estat_population_nuts_2'],
        purpose="POP_COUNTRIES",
        save_file=False
    )
    df_estat_population_subdiv1 = estat(
        code=config.data.estat_tables['estat_population_nuts_2'],
        purpose="POP_SUBDIV1",
        save_file=False
    )
    df_estat_population_subdiv2 = estat(
        code=config.data.estat_tables['estat_population_nuts_2'],
        purpose="POP_SUBDIV2",
        save_file=False
    )
    df_estat_population_agegroups = estat(
        code=config.data.estat_tables['estat_population_agegroups'],
        purpose="POP_AGEGROUPS",
        save_file=False
    )
    df_estat_life_exp = estat(
        code=config.data.estat_tables['estat_life_expectancy'],
        purpose="LIFE_EXP",
        save_file=False
    )
    df_estat_median_age = estat(
        code=config.data.estat_tables['estat_population_structure_indicators'],
        purpose="POP_STRUCT_IND",
        save_file=False
    )
    df_genesis_hospitals_annual = genesis(
        code=config.data.genesis_tables['hospitals_annual'],
        purpose='HOSP_ANNUAL',
        save_file=True,
        path=HOSPITALS_PATH
    )
    df_genesis_hospital_staff_annual = genesis(
        code=config.data.genesis_tables['hospital_staff_annual'],
        purpose='HOSP_STAFF_ANNUAL',
        save_file=True,
        path=HOSPITALS_PATH
    )

    mortalities.estat_death_causes_annual_agegroups(df=df_estat_death_causes_annual_agegroups)
    population.estat_population_countries(df=df_estat_population_countries)
    population.estat_population_subdivision_1(df=df_estat_population_subdiv1)
    population.estat_population_subdivision_2(df=df_estat_population_subdiv2)
    population.estat_population_agegroups(df=df_estat_population_agegroups)
    population.estat_life_exp_at_birth(df=df_estat_life_exp)
    population.estat_median_age(df=df_estat_median_age)
    hospital.genesis_hospitals_annual(df=df_genesis_hospitals_annual)
    hospital.genesis_hospital_staff_annual(df=df_genesis_hospital_staff_annual)


if __name__ == '__main__':
    args = argparse.ArgumentParser()
    args.add_argument('--procedure', default='daily')
    procedure = args.parse_args()._get_kwargs()[0][1]

    if procedure == 'daily':
        print("executing daily procedure...")
        daily()
    elif procedure == 'weekly':
        print("executing weekly procedure...")
        weekly()
    elif procedure == 'annual':
        print("executing annual procedure...")
        annual()
    elif procedure == 'rki_bulk':
        print("executing rki bulk (csv) procedure...")
        rki_bulk()
    elif procedure == 'divi_bulk':
        print("executing divi bulk (csv) procedure...")
        divi_bulk()
    else:
        Exception("Invalid argument! Use 'daily', 'weekly', 'annual', 'rki_bulk' or 'divi_bulk'")
