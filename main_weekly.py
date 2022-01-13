from src.corona import rki_transform
from src.mortality import estat_transform
from src.web_scraper import rki_scrap, estat_scrap
from src.utils import db_helper as database

"""
Runs weekly via batch
"""


def main():
    #tests()
    mortality()


def tests():
    db = database.ProjDB()

    # --Scraping Data--
    df_tests_weekly = rki_scrap.tests_weekly(save_file=True)

    # --Transformation--
    df_tests_weekly = rki_transform.tests_weekly(df_tests_weekly)

    # --DB insert--
    db.insert_or_update(df=df_tests_weekly, table='tests_weekly')

    db.db_close()


def mortality():
    db = database.ProjDB()

    # --Scraping Data--
    df_deaths_weekly = estat_scrap.deaths_weekly_agegroups(save_file=False)
    df_death_causes_annual = estat_scrap.death_causes_annual_agegroups(save_file=False)

    # --Transformation--
    df_deaths_weekly = estat_transform.deaths_weekly_agegroups(df_deaths_weekly)
    df_death_causes_annual = estat_transform.death_causes_annual_agegroups(df_death_causes_annual)

    # --DB insert--
    db.insert_or_update(df=df_deaths_weekly, table='deaths_weekly_agegroups')
    db.insert_or_update(df=df_death_causes_annual, table='death_causes_annual_agegroups')

    db.db_close()


if __name__ == '__main__':
    main()
