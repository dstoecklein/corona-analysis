from table_helpers import countries_helper, calendar_helper
from database.db import Database
from config.core import cfg_db
import table_helpers

database = Database(db_uri=f"{cfg_db.db.dialect}{cfg_db.db.name}.db")



with database.ManagedSessionMaker() as session:
    c = countries_helper.get_subdivision1(session=session, nuts_1="BE1")
    print(type(c))
    print(type(c._mapping))
    #d = calendar_helper.get_calendar_weeks(session, 2051)
    #print(d)




f = {'subdivision_1': 'test1', 'subdiv1_latitude': None, 'subdiv1_longitude': None, 'iso_3166_2': None, 'nuts_1': 'BE1', 'country_en': 'Belgium', 'country_de': 'Belgien', 'country_latitude': 50.503887, 'country_longitude': 4.469936, 'iso_3166_1_alpha2': 'be', 'iso_3166_1_alpha3': 'bel', 'iso_3166_1_numeric': 56, 'nuts_0': 'BE'}

"""
    y = calendar_helper.get_calendar_year(session, 2050)
    print(y.iso_year)
    
    y = calendar_helper.check_iso_key_exist(session, 205053)
    
    
    """
    #print(y[0])
