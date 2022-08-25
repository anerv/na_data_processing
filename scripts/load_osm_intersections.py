'''
Load nodes with intersection tags
Classify intersection tags as uncontrolled/uncontrolled
'''

#%%
import pyrosm
import yaml
import pandas as pd
import json
from src import db_functions as dbf
import pickle

#%%
with open(r'config.yml') as file:
    parsed_yaml_file = yaml.load(file, Loader=yaml.FullLoader)

    use_postgres = parsed_yaml_file['use_postgres']

    osm_fp = parsed_yaml_file['osm_fp']

    ref_id_col = parsed_yaml_file['geodk_id_col']

    crs = parsed_yaml_file['CRS']

    db_name = parsed_yaml_file['db_name']
    db_user = parsed_yaml_file['db_user']
    db_password = parsed_yaml_file['db_password']
    db_host = parsed_yaml_file['db_host']
    db_port = parsed_yaml_file['db_port']
  
print('Settings loaded!')

#%%
osm = pyrosm.OSM(osm_fp)

extra_attr = ['crossing','crossing:island','flashing_lights','traffic_signals']

custom_filter = {
    'highway': ['traffic_signals','crossing'],
    'crossing': ['crossing','controlled','uncontrolled','marked','unmarked','traffic_signals','zebra','islands'],
    'crossing:island': ['yes'],
    'flashing_lights': ['yes','sensor','button','always'],
    'traffic_signals': ['yes']
    }

intersections = osm.get_data_by_custom_criteria(
    osm_keys_to_keep=extra_attr,
    custom_filter=custom_filter, 
    extra_attributes=extra_attr
    )

#%%
# Drop those that are not nodes
intersections = intersections.loc[intersections.osm_type=='node']

# Reproject
intersections.to_crs(crs, inplace=True)

missing_keys = [m for m in extra_attr if m not in intersections.columns]

# Explode tags dict to get cycleway surface
intersections['temp_id'] = intersections.index

intersections_selection = intersections.loc[intersections.tags.notna()].copy()

intersections_selection['dict'] = intersections_selection['tags'].astype(str).apply(lambda x: json.loads(x))
intersections_selection = intersections_selection[['temp_id','dict']]

expl = intersections_selection.dict.apply(pd.Series)

existing_missing_keys = [m for m in missing_keys if m in expl.columns]

if len(existing_missing_keys) > 0:
    expl = expl[missing_keys]
    attr = expl.join(intersections_selection)

    intersections = intersections.merge(attr, on='temp_id', how='left')
    intersections.drop('dict',axis=1,inplace=True)
    intersections.drop('temp_id', axis=1, inplace=True)

else:
    print('Missing keys not found...')
    intersections.drop('temp_id', axis=1, inplace=True)


# Get rid of unnecessary columns
useful_colums = ['id','highway','geometry','traffic_signals','crossing','crossing:island','flashing_lights','proposed','construction','bicycle']
useful_colums = [c for c in useful_colums if c in intersections.columns]
intersections = intersections[useful_colums]

print(f'{len(intersections)} nodes with information on intersections found!')

#%%
# Export data

if use_postgres:

    print('Saving data to PostgreSQL!')

    connection = dbf.connect_pg(db_name, db_user, db_password)

    engine = dbf.connect_alc(db_name, db_user, db_password, db_port=db_port)

    dbf.to_postgis(geodataframe=intersections, table_name='intersection_tags', engine=engine)

    q = 'SELECT id, highway FROM intersection_tags LIMIT 10;'

    test = dbf.run_query_pg(q, connection)

    print(test)

    connection.close()

else:

    print('Saving data to file!')

    with open('../data/osm_intersection_tags.pickle', 'wb') as handle:
        pickle.dump(intersections, handle, protocol=pickle.HIGHEST_PROTOCOL)

#%%
if use_postgres:

    print('Classifying intersection nodes!')

    connection = dbf.connect_pg(db_name, db_user, db_password)

    engine = dbf.connect_alc(db_name, db_user, db_password, db_port=db_port)

    q = 'intersections.sql'

    inter = dbf.run_query_pg(q, connection)

    q = 'SELECT * FROM intersections LIMIT 10;'

    test = dbf.run_query_pg(q, connection)

    print(test)

    connection.close()
#%%