from src.mortality import deaths
from src.mortality import death_causes
from src.corona import rki_transform
from src.web_scraper import rki_scrap, estat_scrap
from src.database import db_helper as database

"""
Runs weekly via batch
"""


def main():
    tests()
    #deaths()


def tests():
    db = database.ProjDB()
    df = rki_scrap.tests_weekly(save_file=True)
    df_rki_tests = rki_transform.tests_weekly(df)
    db.insert_or_update(df=df_rki_tests, table='tests_weekly')
    db.db_close()

#def deaths():
    # eurostat
    #deaths.weekly_deaths('deaths_agegroups_10y_weekly', ['DE', 'SE'], 2010)
    #death_causes.annual_death_causes('death_causes_agegroups_10y_annual', ['DE', 'SE'])


if __name__ == '__main__':
    main()