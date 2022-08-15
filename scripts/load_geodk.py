'''
Script for loading GeoDK data (shapefile, geopackage etc.) to PostGIS db
Required an existing db with postgis extension
'''
#%%
import geopandas as gpd
import yaml
from src import db_functions as dbf
import pickle
# from src import matching_functions as mf
import osmnx as ox
from src import graph_functions as gf
from src import simplification_functions as sf
# %%
with open(r'config.yml') as file:
    parsed_yaml_file = yaml.load(file, Loader=yaml.FullLoader)

    use_postgres = parsed_yaml_file['use_postgres']

    geodk_fp = parsed_yaml_file['geodk_fp']
    geodk_id_col = parsed_yaml_file['geodk_id_col']

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

geodk = geodk.to_crs(crs)

assert geodk.crs == crs

assert len(geodk) == len(geodk[geodk_id_col].unique())

#%%
# Get cycling infrastructure
geodk_bike = geodk.loc[geodk.vejklasse.isin(['Cykelsti langs vej','Cykelbane langs vej'])].copy()

#%%
# Create graph structure
graph_ref = gf.create_osmnx_graph(geodk_bike)

#%%
G_sim = sf.momepy_simplify_graph(graph_ref, attributes=['vejklasse'])

# Check crs
nodes, edges = ox.graph_to_gdfs(G_sim)
assert edges.crs == crs, 'Data is in wrong crs!'

# #%%
# Create unique id
edges['old_id'] = edges.fot_id

edges.reset_index(inplace=True)

ids = []
for i in range(1000, 1000+len(edges)):
    ids.append(i)

edges['edge_id'] = ids

assert len(edges.edge_id.unique()) == len(edges)

#%%
if use_postgres:

    connection = dbf.connect_pg(db_name, db_user, db_password)

    engine = dbf.connect_alc(db_name, db_user, db_password, db_port=db_port)

    dbf.to_postgis(geodataframe=geodk_bike, table_name='geodk_bike', engine=engine)
    dbf.to_postgis(geodataframe=edges, table_name='geodk_bike_simple', engine=engine)
    dbf.to_postgis(geodataframe=nodes, table_name='geo_dk_nodes_simple', engine=engine)

    q = 'SELECT fot_id, feat_type FROM geodk_bike LIMIT 10;'
    q2 = 'SELECT fot_id, feat_type FROM geodk_bike_simple LIMIT 10;'

    test = dbf.run_query_pg(q, connection)

    print(test)

    test2 = dbf.run_query_pg(q2, connection)

    print(test2)

    connection.close()

else:
 
    with open('../data/reference_data.pickle', 'wb') as handle:
        pickle.dump(graph_ref, handle, protocol=pickle.HIGHEST_PROTOCOL)

    with open('../data/reference_data_simple.pickle', 'wb') as handle:
        pickle.dump(G_sim, handle, protocol=pickle.HIGHEST_PROTOCOL)
    

# %%
