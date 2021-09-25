import src.web_scraper.estat_scrap as estat_scrap
import src.web_scraper.rki_scrap as rki_scrap
import src.web_scraper.oecd_scrap as oecd_scrap
import src.corona.rki_transform as rki_transform
import src.corona.estat_transform as estat_transform
import src.corona.oecd_transform as oecd_transform

"""
Runs weekly via batch
"""

# raw data
estat_scrap.weekly_deaths(insert_into='estat_weekly_deaths')
estat_scrap.annual_death_causes(insert_into='estat_annual_death_causes')
oecd_scrap.weekly_deaths(insert_into='oecd_weekly_deaths')

# transformed data
estat_transform.weekly_deaths('estat_weekly_deaths_ger', 'DE', 2016)  # germany
estat_transform.weekly_deaths('estat_weekly_deaths_swe', 'SE', 2010)  # sweden
estat_transform.annual_deaths_causes('estat_annual_death_causes_ger', 'DE')  # germany
estat_transform.annual_deaths_causes('estat_annual_death_causes_swe', 'SE')  # sweden
oecd_transform.weekly_deaths('oecd_weekly_deaths_swe', 'SWE')  # sweden
oecd_transform.weekly_deaths('oecd_weekly_deaths_isr', 'ISR')  # israel
oecd_transform.weekly_covid_deaths('oecd_weekly_covid_deaths_swe', 'SWE')  # sweden
oecd_transform.weekly_covid_deaths('oecd_weekly_covid_deaths_isr', 'ISR')  # israel

# RKI raw + transformed data
rki_transform.weekly_tests(
    rki_scrap.weekly_tests(save_file=False),
    insert_into='rki_weekly_tests_ger'
)

rki_transform.weekly_tests_states(
    rki_scrap.weekly_tests_states(save_file=False),
    insert_into='rki_weekly_tests_states_ger'
)
