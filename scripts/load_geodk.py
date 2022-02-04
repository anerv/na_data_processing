'''
Script for loading GeoDK data (shapefile, geopackage etc.) to PostGIS db
Required an exisinting db with postgis extension
'''

#%%
import geopandas as gpd
import yaml
from src import db_functions as dbf
# %%
with open(r'config.yml') as file:
    parsed_yaml_file = yaml.load(file, Loader=yaml.FullLoader)


    study_area = parsed_yaml_file['study_area']
    
    geodk_fp = parsed_yaml_file['geodk_fp']

    crs = parsed_yaml_file['CRS']

    db_name = parsed_yaml_file['db_name']
    db_user = parsed_yaml_file['db_user']
    db_password = parsed_yaml_file['db_password']
    db_host = parsed_yaml_file['db_host']
    db_port = parsed_yaml_file['db_port']
  
    
print('Settings loaded!')
#%%
geodk = gpd.read_file(geodk_fp)

geodk.columns = geodk.columns.str.lower()

useful_cols = ['fot_id', 'mob_id', 'feat_kode', 'feat_type', 'featstatus',
       'geomstatus', 'startknude', 'slutknude', 'niveau', 'overflade',
       'rund_koer', 'kom_kode', 'vejkode', 'tilfra_koe',
       'trafikart', 'vejklasse', 'vej_mynd', 'vej_type', 'geometry']

geodk = geodk[useful_cols]

#Check crs
assert geodk.crs == crs
#%%
connection = dbf.connect_pg(db_name, db_user, db_password)

engine = dbf.connect_alc(db_name, db_user, db_password, db_port=db_port)

dbf.to_postgis(geodataframe=geodk, table_name='vm_brudt', engine=engine)

#%%
q = 'SELECT fot_id, feat_type FROM vm_brudt LIMIT 10;'

test = dbf.run_query_pg(q, connection)

print(test)

connection.close()
# %%
