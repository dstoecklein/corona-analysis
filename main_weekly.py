from src.mortality import deaths
from src.mortality import death_causes
from src.corona import rki_transform
from src.web_scraper import rki_scrap

"""
Runs weekly via batch
"""

# eurostat
deaths.weekly_deaths('deaths_by_agegroups_weekly', ['DE', 'SE'], 2010)
death_causes.annual_death_causes('death_causes_by_agegroups_annual', ['DE', 'SE'])

# rki
df = rki_scrap.weekly_tests(save_file=False)
rki_transform.weekly_tests(df=df, table='tests_weekly')
