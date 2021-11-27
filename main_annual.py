from src.database import db_helper as database
from src.utils import paths, estat_helper
from src.web_scraper import estat_scrap
from src.population import estat_transform

COVID_PATH = paths.get_covid19_ger_path()
HOSP_PATH = paths.get_hospitals_ger_path()

"""
Runs when needed or annual
"""


def main():
    db = database.ProjDB()

    # --Scraping Data--
    df_population_nuts_2 = estat_scrap.population_nuts_2(save_file=True)

    # --Pre-Processing--
    df_population_nuts_2 = estat_helper.pre_process_population(df_population_nuts_2)

    # --Transformation--
    population_countries = estat_transform.population_countries(df_population_nuts_2)
    population_subdivision_1 = estat_transform.population_subdivision_1(df_population_nuts_2)
    population_subdivision_2 = estat_transform.population_subdivision_2(df_population_nuts_2)

    # --DB insert--
    db.insert_or_update(df=population_countries, table='population_countries')
    db.insert_or_update(df=population_subdivision_1, table='population_subdivs_1')
    db.insert_or_update(df=population_subdivision_2, table='population_subdivs_2')

    db.db_close()


if __name__ == '__main__':
    main()
