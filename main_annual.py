from src.database import db_helper as database
from src.utils import paths
from src.web_scraper import estat_scrap, genesis_scrap
from src.population import estat_transform
from src.hospitals import genesis_transform

COVID_PATH = paths.get_covid_path()
HOSP_PATH = paths.get_hospitals_path()

"""
Runs when needed or annualy
"""


def main():
    hospitals()
    population()


def hospitals():
    db = database.ProjDB()

    # --Scraping Data--
    df_hospitals_annual = genesis_scrap.hospitals_annual(save_file=True)
    df_hospital_staff_annual = genesis_scrap.hospital_staff_annual(save_file=True)

    # --Transformation--
    df_hospitals_annual = genesis_transform.hospitals_annual(df_hospitals_annual)
    df_hospital_staff_annual = genesis_transform.hospital_staff_annual(df_hospital_staff_annual)

    # --DB insert--
    db.insert_or_update(df=df_hospitals_annual, table='hospitals_annual')
    db.insert_or_update(df=df_hospital_staff_annual, table='hospital_staff_annual')

    db.db_close()


def population():
    db = database.ProjDB()

    # --Scraping Data--
    df_population_nuts_2 = estat_scrap.population_nuts_2(save_file=False)
    df_population_agegroups = estat_scrap.population_agegroups(save_file=False)
    df_life_expectancy = estat_scrap.life_expectancy(save_file=False)

    # --Transformation--
    df_population_countries = estat_transform.population_countries(df_population_nuts_2)
    df_population_subdivision_1 = estat_transform.population_subdivision_1(df_population_nuts_2)
    df_population_subdivision_2 = estat_transform.population_subdivision_2(df_population_nuts_2)
    df_population_countries_agegroups_10y = estat_transform.population_agegroups(df_population_agegroups)
    df_life_expectancy = estat_transform.pre_process_life_exp_at_birth(df_life_expectancy)

    # --DB insert--
    db.insert_or_update(df=df_population_countries, table='population_countries')
    db.insert_or_update(df=df_population_subdivision_1, table='population_subdivs_1')
    db.insert_or_update(df=df_population_subdivision_2, table='population_subdivs_2')
    db.insert_or_update(df=df_population_countries_agegroups_10y, table='population_countries_agegroups')
    df_life_expectancy.to_csv("df_life_expectancy.csv", index=False, sep=";")
    db.insert_or_update(df=df_life_expectancy, table='life_expectancy')

    db.db_close()


if __name__ == '__main__':
    main()
