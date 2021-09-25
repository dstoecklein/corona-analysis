import src.web_scraper.rki_scrap as rki_scrap
import src.web_scraper.owid_scrap as owid_scrap
import src.corona.rki_transform as rki_transform
import src.corona.owid_transform as owid_transform

"""
Runs daily via batch
"""

# raw data
owid_scrap.daily_covid(insert_into='owid_daily_covid')
rki_scrap.daily_covid(save_file=True)

# transformed data
owid_transform.daily_covid(insert_into='owid_daily_covid_swe', country_code='SWE')  # sweden
owid_transform.daily_covid(insert_into='owid_daily_covid_isr', country_code='ISR')  # israel
rki_transform.main()  # germany, runs all daily covid calculations

# raw + transformed data
rki_transform.daily_rvalue(  # germany
    rki_scrap.daily_rvalue(save_file=False),
    insert_into='rki_daily_rvalue_ger'
)
