'''
Classifies all OSM edges and fills out missing values
'''

#%%
import yaml
from src import db_functions as dbf

#%%

with open(r'config.yml') as file:
    parsed_yaml_file = yaml.load(file, Loader=yaml.FullLoader)

    use_postgres = parsed_yaml_file['use_postgres']

    osm_fp = parsed_yaml_file['osm_fp']
    geodk_fp = parsed_yaml_file['geodk_fp']

    crs = parsed_yaml_file['CRS']

    db_name = parsed_yaml_file['db_name']
    db_user = parsed_yaml_file['db_user']
    db_password = parsed_yaml_file['db_password']
    db_host = parsed_yaml_file['db_host']
    db_port = parsed_yaml_file['db_port']
  
print('Settings loaded!')
#%%

print('Classifying edges...')

connection = dbf.connect_pg(db_name, db_user, db_password)

engine = dbf.connect_alc(db_name, db_user, db_password, db_port=db_port)

q = '../sql/classify_bicycle_infra.sql'

classify = dbf.run_query_pg(q, connection)

connection = dbf.connect_pg(db_name, db_user, db_password)

q = 'SELECT edge_id, protected FROM osm_edges_simplified WHERE protected = true LIMIT 10;'

test = dbf.run_query_pg(q, connection)

print(test)

#%%

print('Interpolating missing attributes...')

connection = dbf.connect_pg(db_name, db_user, db_password)

engine = dbf.connect_alc(db_name, db_user, db_password, db_port=db_port)

q = '../sql/fill_missing_values.sql'

interpolate = dbf.run_query_pg(q, connection)

connection = dbf.connect_pg(db_name, db_user, db_password)

q = "SELECT edge_id, protected FROM osm_edges_simplified WHERE lit_as = 'yes' LIMIT 10;"

test = dbf.run_query_pg(q, connection)

print(test)

#%%