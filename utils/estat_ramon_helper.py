# Helper script to make a relational table schema out of eurostats Ramon NUTS .csv file
# https://ec.europa.eu/eurostat/ramon/index.cfm?TargetUrl=DSP_PUB_WELC
# Todo: Make this less messy, define methods etc...

import pandas as pd

from src.utils import db_helper as database

# create db connection
db = database.ProjDB()

df = pd.read_csv("nuts.csv", engine='python', sep=';', encoding='utf8')

df_countries = df.copy().loc[df['Level'] == 2]
df_countries = db.merge_countries_fk(df=df_countries, left_on='NUTS-Code', country_code='nuts_0')
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
    columns={'Description': 'subdivision_1', 'NUTS-Code': 'nuts_1'},
    inplace=True
)
df_nuts1 = df_nuts1[~df_nuts1['subdivision_1'].str.contains("Extra-Regio NUTS 1")]
df_nuts1 = df_nuts1.drop(['Parent', 'Code'], axis=1)

# nuts2
df_regions2 = df_regions2.copy().drop(['Order', 'Level'], axis=1)
df_nuts2 = df_regions2.merge(df_nuts1,
                             left_on='Parent',
                             right_on='id1',
                             how='left',
                             )
df_nuts2.rename(
    columns={'Code': 'id2', 'Description': 'subdivision_2', 'NUTS-Code': 'nuts_2'},
    inplace=True
)
df_nuts2 = df_nuts2[~df_nuts2['nuts_2'].str.contains("Extra-Regio NUTS 2")]
df_nuts2 = df_nuts2.drop(['Parent', 'id1', 'subdivision_1', 'countries_fk'], axis=1)

# nuts 3
df_regions3 = df_regions3.copy().drop(['Order', 'Level'], axis=1)
df_nuts3 = df_regions3.merge(df_nuts2,
                             left_on='Parent',
                             right_on='id2',
                             how='left',
                             )
df_nuts3.rename(
    columns={'Code': 'id3', 'Description': 'subdivision_3', 'NUTS-Code': 'nuts_3'},
    inplace=True
)
df_nuts3 = df_nuts3[~df_nuts3['nuts_3'].str.contains("Extra-Regio NUTS 3")]
df_nuts3 = df_nuts3.drop(['Parent', 'id2', 'subdivision_2', 'nuts_1'], axis=1)

del df_nuts1['id1']
# df_nuts1.to_sql('_country_subdivs_1', db.connection, if_exists='append', index=False)


# get foreign keys from database, starting with nuts2 since nuts1 is the root table
tmp = db.get_table('_country_subdivs_1')
df_final2 = tmp.merge(df_nuts2,
                      left_on='nuts_1',
                      right_on='nuts_1',
                      how='left',
                      ).drop(columns=['countries_fk', 'subdivision_1', 'id2', 'nuts_1', 'iso_3166_2'])
df_final2.rename(
    columns={'country_subdivs_1_id': 'country_subdivs_1_fk'},
    inplace=True
)

# df_final2.to_sql('_country_subdivs_2', db.connection, if_exists='append', index=False)


tmp = db.get_table('_country_subdivs_2')
df_final3 = tmp.merge(df_nuts3,
                      left_on='nuts_2',
                      right_on='nuts_2',
                      how='left',
                      ).drop(columns=['country_subdivs_1_fk', 'subdivision_2', 'id3', 'nuts_2'])
df_final3.rename(
    columns={'country_subdivs_2_id': 'country_subdivs_2_fk'},
    inplace=True
)

df_final3.to_sql('_country_subdivs_3', db.connection, if_exists='append', index=False)

db.db_close()
