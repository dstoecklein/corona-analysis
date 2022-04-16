import datetime as dt
from config import core
from config.core import config

"""
Runs daily via batch
"""

# Constants
COVID_FILES_PATH = core.FILES_PATH / 'covid'
COVID_TEST_FILES_PATH = core.FILES_PATH / 'covid_tests'
COVID_RVALUE_FILES_PATH = core.FILES_PATH / 'covid_rvalue'
COVID_VACC_FILES_PATH = core.FILES_PATH / 'covid_vaccinations'
ITCU_FILES_PATH = core.FILES_PATH / 'itcus'


""" def main():
    rki_procedure()
    #divi_procedure(config_path)
    # rki_bulk_procedure()
    # divi_bulk_procedure() """


""" def divi_procedure():
    db = database.ProjDB()

    # --Scraping Data--
    df_divi_counties = divi_scrap.itcu_daily_counties(save_file=True)
    df_divi_states = divi_scrap.itcu_daily_states(save_file=True)

    # --Transformation--
    df_divi_counties = divi_transform.itcu_daily_counties(df=df_divi_counties)
    df_divi_states = divi_transform.itcu_daily_states(df=df_divi_states)

    # --DB insert--
    db.insert_or_update(df=df_divi_counties, table='itcu_daily_counties')
    db.insert_or_update(df=df_divi_states, table='itcu_daily_states')

    db.db_close() """


""" def rki_procedure():
    db = database.ProjDB()

    # --Scraping Data--
    df = rki_scrap.covid_daily(save_file=True)
    df_rvalue = rki_scrap.rvalue_daily(save_file=False)
    df_vaccinations_daily_states = rki_scrap.vaccinations_daily_stastes(save_file=False)

    today = dt.date.today()
    today = dt.datetime(today.year, today.month, today.day)

    # --Transformation--
    df_rki_daily = rki_transform.covid_daily(df=df, date=today)
    df_rki_daily_states = rki_transform.covid_daily_states(df=df, date=today)
    df_rki_daily_counties = rki_transform.covid_daily_counties(df=df, date=today)
    df_rki_daily_agegroups = rki_transform.covid_daily_agegroups(df=df, date=today)
    df_rki_weekly_cumulative = rki_transform.covid_weekly_cummulative(df=df)
    df_rvalue = rki_transform.rvalue_daily(df=df_rvalue)
    df_vaccinations_daily_states = rki_transform.vaccinations_daily_states(df=df_vaccinations_daily_states)

    # --DB insert--
    db.insert_or_update(df=df_rki_daily, table='covid_daily')
    db.insert_or_update(df=df_rki_daily_states, table='covid_daily_states')
    db.insert_or_update(df=df_rki_daily_counties, table='covid_daily_counties')
    db.insert_or_update(df=df_rki_daily_agegroups, table='covid_daily_agegroups')
    db.insert_or_update(df=df_rki_weekly_cumulative, table='covid_weekly_cumulative')
    db.insert_or_update(df_rvalue, table='rvalue_daily')
    db.insert_or_update(df=df_vaccinations_daily_states, table='vaccinations_daily_states')
    db.db_close() """


""" def rki_bulk_procedure():
    db = database.ProjDB()

    for filename in os.listdir(COVID_FILES_PATH):
        if filename.endswith('.csv') or filename.endswith('.xz'):
            extract = re.search(r'\d{4}-\d{2}-\d{2}', filename)
            date = dt.datetime.strptime(extract.group(), '%Y-%m-%d')

            try:
                df = pd.read_csv(COVID_FILES_PATH + filename, engine='python', sep=',', encoding='utf8')
            except UnicodeDecodeError:
                df = pd.read_csv(COVID_FILES_PATH + filename, engine='python', sep=',', encoding='ISO-8859-1')

            # --Transformation--
            df_rki_daily = rki_transform.covid_daily(df=df, date=date)
            df_rki_daily_states = rki_transform.covid_daily_states(df=df, date=date)
            df_rki_daily_counties = rki_transform.covid_daily_counties(df=df, date=date)
            df_rki_daily_agegroups = rki_transform.covid_daily_agegroups(df=df, date=date)
            df_rki_weekly_cumulative = rki_transform.covid_weekly_cummulative(df=df)

            # --DB insert--
            db.insert_or_update(df=df_rki_daily, table='covid_daily')
            db.insert_or_update(df=df_rki_daily_states, table='covid_daily_states')
            db.insert_or_update(df=df_rki_daily_counties, table='covid_daily_counties')
            db.insert_or_update(df=df_rki_daily_agegroups, table='covid_daily_agegroups')
            db.insert_or_update(df=df_rki_weekly_cumulative, table='covid_weekly_cumulative')

    db.db_close() """


""" def divi_bulk_procedure():
    db = database.ProjDB()

    for filename in os.listdir(HOSP_FILES_PATH):
        if filename.endswith('.csv') or filename.endswith('.xz'):

            try:
                df = pd.read_csv(HOSP_FILES_PATH + filename, engine='python', sep=',', encoding='utf8')
            except UnicodeDecodeError:
                df = pd.read_csv(HOSP_FILES_PATH + filename, engine='python', sep=',', encoding='ISO-8859-1')

            if '_COUNTIES' in filename:
                df_divi_counties = divi_transform.itcu_daily_counties(df=df)
                db.insert_or_update(df=df_divi_counties, table='itcu_daily_counties')
            elif '_STATES' in filename:
                df_divi_states = divi_transform.itcu_daily_states(df=df)
                db.insert_or_update(df=df_divi_states, table='itcu_daily_states')
            else:
                return

    db.db_close() """


from src.get_data import rki, estat, divi, genesis
from src import covid, covid_rvalue, covid_tests, covid_vaccinations, intensive_care_units
#rki_daily, rki_daily_states, rki_daily_counties, rki_daily_agegroups, rki_weekly_cumulative

TODAY = dt.date.today()
TODAY = dt.datetime(TODAY.year, TODAY.month, TODAY.day)
if __name__ == '__main__':
    """
    df_rki_covid_daily = rki(
        url=config.data.urls['rki_covid_daily'],
        purpose='RKI_COVID19_DAILY',
        save_file=True,
        path=COVID_FILES_PATH
    )
    df_rki_tests_weekly = rki(
        url=config.data.urls['rki_tests_weekly'],
        purpose='RKI_TESTS_WEEKLY',
        save_file=True,
        path=COVID_TEST_FILES_PATH,
        is_excel=True,
        sheet_name='1_Testzahlerfassung'
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
    )"""
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
    
    # remove from daily
    covid_tests.rki_weekly(df=df_rki_tests_weekly)
    
    #covid_vaccinations.rki_vaccinations_daily_cumulative(df=df_rki_vacc_daily_cumulative)
    covid_vaccinations.rki_vaccinations_daily_states(df=df_rki_vacc_daily_states)
    
    intensive_care_units.divi_daily_counties(df=df_divi_itcu_daily_counties)
    """
    intensive_care_units.divi_daily_states(df=df_divi_itcu_daily_states)