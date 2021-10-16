import src.mortality.deaths as deaths
import src.mortality.death_causes as death_causes

"""
Runs weekly via batch
"""

# transformed data
deaths.weekly_deaths('deaths_by_agegroups_weekly', ['DE', 'SE'], 2010)
death_causes.annual_death_causes('death_causes_by_agegroups_annual', ['DE', 'SE'])

# oecd_transform.weekly_deaths('oecd_weekly_deaths_swe', 'SWE')  # sweden
# oecd_transform.weekly_deaths('oecd_weekly_deaths_isr', 'ISR')  # israel
# oecd_transform.weekly_covid_deaths('oecd_weekly_covid_deaths_swe', 'SWE')  # sweden
# oecd_transform.weekly_covid_deaths('oecd_weekly_covid_deaths_isr', 'ISR')  # israel
#
# # RKI raw + transformed data
# rki_transform.weekly_tests(
#     rki_scrap.weekly_tests(save_file=False),
#     insert_into='rki_weekly_tests_ger'
# )
#
# rki_transform.weekly_tests_states(
#     rki_scrap.weekly_tests_states(save_file=False),
#     insert_into='rki_weekly_tests_states_ger'
# )
