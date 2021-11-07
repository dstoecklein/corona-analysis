import pandas as pd
from src.database import db_helper as database

# create db connection
db = database.ProjDB()

df = pd.read_csv("nuts.csv", engine='python', sep=';', encoding='utf8')

df_countries = df.loc[df['Level'] == 2]
df_regions = df.loc[df['Level'] == 3]
df_regions2 = df.loc[df['Level'] == 4]
df_regions3 = df.loc[df['Level'] == 5]

df_regions = df_regions.drop(['Order', 'Level'], axis=1)
df_regions2 = df_regions2.drop(['Order', 'Level'], axis=1)
df_regions3 = df_regions3.drop(['Order', 'Level'], axis=1)

df_countries = db.merge_countries_fk(df=df_countries, left_on='NUTS-Code', country_code='nuts_code')
df_countries = df_countries.drop(['Order', 'Level', 'Parent', 'NUTS-Code', 'Description'], axis=1)

df_nuts1 = df_countries.merge(df_regions,
                              left_on='Code',
                              right_on='Parent',
                              how='left',
                              )

#TODO:
df_regions.to_csv("regions.csv", sep=";", index=False)
df_regions2.to_csv("regions2.csv", sep=";", index=False)
df_nuts2 = df_regions.merge(df_regions2,
                              left_on='Code',
                              right_on='Parent',
                              how='left',
                              )
df_nuts2.to_csv("nuts2.csv", sep=";", index=False)

df_nuts3 = df_regions2.merge(df_regions3,
                              left_on='Code',
                              right_on='Parent',
                              how='left',
                              )

# finishing nuts1
df_nuts1 = df_nuts1.drop(['Code', 'Parent'], axis=1)
df_nuts1.rename(
    columns={'Description': 'nuts1', 'NUTS-Code': 'nuts1_code'},
    inplace=True
)
df_nuts1 = df_nuts1[~df_nuts1['nuts1'].str.contains("Extra-Regio NUTS 1")]
#df_nuts1.to_sql('_territory_nuts1', db.connection, if_exists='append', index=False)


db.db_close()
