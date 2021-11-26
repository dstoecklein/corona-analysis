from src.mortality import deaths
from src.mortality import death_causes
from src.corona import rki_transform
from src.web_scraper import rki_scrap

"""
Runs weekly via batch
"""
#TODO: Wie daily anpassen
# eurostat
deaths.weekly_deaths('deaths_agegroups_10y_weekly', ['DE', 'SE'], 2010)
death_causes.annual_death_causes('death_causes_agegroups_10y_annual', ['DE', 'SE'])

# rki
df = rki_scrap.tests_weekly(save_file=False)
rki_transform.tests_weekly(df=df, table='tests_weekly')
