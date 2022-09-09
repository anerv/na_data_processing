'''
Classifies all OSM edges and intersection nodes and fills out missing values for edges
'''

#%%
import yaml
from src import db_functions as dbf

#%%

with open(r'../config.yml') as file:

    parsed_yaml_file = yaml.load(file, Loader=yaml.FullLoader)

    osm_fp = parsed_yaml_file['osm_fp']
    geodk_fp = parsed_yaml_file['geodk_fp']

    crs = parsed_yaml_file['CRS']

    h3_urban_level = parsed_yaml_file['h3_urban_level']

    db_name = parsed_yaml_file['db_name']
    db_user = parsed_yaml_file['db_user']
    db_password = parsed_yaml_file['db_password']
    db_host = parsed_yaml_file['db_host']
    db_port = parsed_yaml_file['db_port']
  
print('Settings loaded!')
#%%

print('Classifying intersection nodes!')

connection = dbf.connect_pg(db_name, db_user, db_password)

q = 'sql/intersections.sql'

inter = dbf.run_query_pg(q, connection)

q = 'SELECT osmid, count, inter_type FROM intersections WHERE inter_type IS NOT NULL LIMIT 10;'

test = dbf.run_query_pg(q, connection)

print(test)

connection.close()

#%%

print('Classifying edges...')

connection = dbf.connect_pg(db_name, db_user, db_password)

engine = dbf.connect_alc(db_name, db_user, db_password, db_port=db_port)

q = 'sql/classify_bicycle_infra.sql'

classify = dbf.run_query_pg(q, connection)

connection = dbf.connect_pg(db_name, db_user, db_password)

q = "SELECT edge_id, protected FROM osm_edges_simplified WHERE protected = 'true' LIMIT 10;"

test = dbf.run_query_pg(q, connection)

print(test)

#%%
# Classify edges as urban/rural etc

print('Classifying edges as urban/non-urban...')

connection = dbf.connect_pg(db_name, db_user, db_password)

create_view = f'''
CREATE VIEW urban_nodes AS 
(SELECT
    polys.urban,
    polys.urban_code,
    nodes.osmid
FROM urban_polygons_{h3_urban_level} AS polys
JOIN osm_nodes_simplified AS nodes
ON ST_Intersects(polys.geometry, nodes.geometry));
'''

view = dbf.run_query_pg(create_view, connection)

classify_urban = 'sql/classify_urban_network.sql'
classify = dbf.run_query_pg(classify_urban, connection)

drop = dbf.run_query_pg('DROP VIEW urban_nodes;', connection)

connection.close()

#%%
print('Interpolating missing attributes...')

connection = dbf.connect_pg(db_name, db_user, db_password)

engine = dbf.connect_alc(db_name, db_user, db_password, db_port=db_port)

q = 'sql/fill_missing_values.sql'

interpolate = dbf.run_query_pg(q, connection)

connection = dbf.connect_pg(db_name, db_user, db_password)

q = "SELECT edge_id, protected FROM osm_edges_simplified WHERE lit_as = 'yes' LIMIT 10;"

test = dbf.run_query_pg(q, connection)

print(test)

#%%
print('Create cycling network...')

connection = dbf.connect_pg(db_name, db_user, db_password)

engine = dbf.connect_alc(db_name, db_user, db_password, db_port=db_port)

q = 'sql/create_cycling_network.sql'

interpolate = dbf.run_query_pg(q, connection)

connection = dbf.connect_pg(db_name, db_user, db_password)

q = "SELECT edge_id, cycling_infra_new FROM cycling_edges LIMIT 10;"

test = dbf.run_query_pg(q, connection)

print(test)