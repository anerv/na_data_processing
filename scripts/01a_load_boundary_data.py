'''
Loads layer with municipal boundaries to database
'''

#%%
import geopandas as gpd
import yaml
from src import db_functions as dbf
#%%
with open(r'../config.yml') as file:
    parsed_yaml_file = yaml.load(file, Loader=yaml.FullLoader)

    muni_fp = parsed_yaml_file['muni_fp']

    crs = parsed_yaml_file['CRS']

    db_name = parsed_yaml_file['db_name']
    db_user = parsed_yaml_file['db_user']
    db_password = parsed_yaml_file['db_password']
    db_host = parsed_yaml_file['db_host']
    db_port = parsed_yaml_file['db_port']
  
    
print('Settings loaded!')


#read muni data
muni = gpd.read_file(muni_fp)

assert muni.crs == crs

muni.columns = muni.columns.str.lower()

useful_cols = ['id.lokalid', 'status', 'geometristatus', 
        'dagiid', 'navn','landekode', 'skala',
        'kommunekode', 'lau1vaerdi', 'udenforkommuneinddeling',
        'regionskode', 'regionslokalid', 'region', 'geometry']

muni = muni[useful_cols]


connection = dbf.connect_pg(db_name, db_user, db_password)

engine = dbf.connect_alc(db_name, db_user, db_password, db_port=db_port)

dbf.to_postgis(geodataframe=muni, table_name='muni_boundaries', engine=engine)


q1 = 'SELECT navn, kommunekode FROM muni_boundaries LIMIT 10;'

test1 = dbf.run_query_pg(q1, connection)

print(test1)
