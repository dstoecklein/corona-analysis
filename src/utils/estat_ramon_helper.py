# Helper script to make a relational table schema out of eurostats Ramon NUTS .csv file
# https://ec.europa.eu/eurostat/ramon/index.cfm?TargetUrl=DSP_PUB_WELC
# Todo: Make this less messy, define methods etc...

import pandas as pd
from src.database import db_helper as database

# create db connection
db = database.ProjDB()

df = pd.read_csv("nuts.csv", engine='python', sep=';', encoding='utf8')

df_countries = df.copy().loc[df['Level'] == 2]
df_countries = db.merge_countries_fk(df=df_countries, left_on='NUTS-Code', country_code='nuts_code')
df_countries = df_countries.drop(['Order', 'Level', 'Parent', 'NUTS-Code', 'Description'], axis=1)

df_regions1 = df.copy().loc[df['Level'] == 3]
df_regions2 = df.copy().loc[df['Level'] == 4]
df_regions3 = df.copy().loc[df['Level'] == 5]

# nuts1
df_regions1 = df_regions1.copy().drop(['Order', 'Level'], axis=1)
df_regions1.rename(
    columns={'Code': 'id1'},
    inplace=True
)

df_nuts1 = df_regions1.merge(df_countries,
                             left_on='Parent',
                             right_on='Code',
                             how='left',
                             )
df_nuts1.rename(
    columns={'Description': 'nuts1', 'NUTS-Code': 'nuts1_code'},
    inplace=True
)
df_nuts1 = df_nuts1[~df_nuts1['nuts1'].str.contains("Extra-Regio NUTS 1")]
df_nuts1 = df_nuts1.drop(['Parent', 'Code'], axis=1)

# nuts2
df_regions2 = df_regions2.copy().drop(['Order', 'Level'], axis=1)
df_nuts2 = df_regions2.merge(df_nuts1,
                             left_on='Parent',
                             right_on='id1',
                             how='left',
                             )
df_nuts2.rename(
    columns={'Code': 'id2', 'Description': 'nuts2', 'NUTS-Code': 'nuts2_code'},
    inplace=True
)
df_nuts2 = df_nuts2[~df_nuts2['nuts2'].str.contains("Extra-Regio NUTS 2")]
df_nuts2 = df_nuts2.drop(['Parent', 'id1', 'nuts1', 'countries_fk'], axis=1)

# nuts 3
df_regions3 = df_regions3.copy().drop(['Order', 'Level'], axis=1)
df_nuts3 = df_regions3.merge(df_nuts2,
                             left_on='Parent',
                             right_on='id2',
                             how='left',
                             )
df_nuts3.rename(
    columns={'Code': 'id3', 'Description': 'nuts3', 'NUTS-Code': 'nuts3_code'},
    inplace=True
)
df_nuts3 = df_nuts3[~df_nuts3['nuts3'].str.contains("Extra-Regio NUTS 3")]
df_nuts3 = df_nuts3.drop(['Parent', 'id2', 'nuts2', 'nuts1_code'], axis=1)

# get foreign keys from database, starting with nuts2 since nuts1 is the root table
tmp = db.get_table('_territory_nuts1')
df_final2 = tmp.merge(df_nuts2,
                      left_on='nuts1_code',
                      right_on='nuts1_code',
                      how='left',
                      ).drop(columns=['countries_fk', 'nuts1', 'id2', 'nuts1_code'])
df_final2.rename(
    columns={'ID': 'territory_nuts1_fk'},
    inplace=True
)

# df_final2.to_sql('_territory_nuts2', db.connection, if_exists='append', index=False)


tmp = db.get_table('_territory_nuts2')
df_final3 = tmp.merge(df_nuts3,
                      left_on='nuts2_code',
                      right_on='nuts2_code',
                      how='left',
                      ).drop(columns=['territory_nuts1_fk', 'nuts2_code', 'nuts2', 'id3', 'nuts2_code'])
df_final3.rename(
    columns={'ID': 'territory_nuts2_fk'},
    inplace=True
)

# df_final3.to_sql('_territory_nuts3', db.connection, if_exists='append', index=False)

db.db_close()
