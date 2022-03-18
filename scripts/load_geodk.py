'''
Script for loading GeoDK data (shapefile, geopackage etc.) to PostGIS db
Required an exisinting db with postgis extension
'''

#%%
import geopandas as gpd
import yaml
from src import db_functions as dbf
import pickle
from src import matching_functions as mf
import osmnx as ox
from src import simplification_functions as sf
# %%
with open(r'config.yml') as file:
    parsed_yaml_file = yaml.load(file, Loader=yaml.FullLoader)

    use_postgres = parsed_yaml_file['use_postgres']

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

geodk = geodk.to_crs(crs)

assert geodk.crs == crs

#%%
# Create graph structure
graph_ref = mf.create_osmnx_graph(geodk)

# Works, but changes some edge geometries??
G_sim = sf.simplify_graph(graph_ref, attributes = ['vejklasse'])

#%%
reg_nodes, reg_edges = ox.graph_to_gdfs(graph_ref)

sim_nodes, sim_edges = ox.graph_to_gdfs(G_sim)

sim_nodes.to_file('../data/sim_nodes.gpkg', driver='GPKG')
sim_edges[['geometry','vejklasse']].to_file('../data/sim_edges.gpkg', driver='GPKG')
reg_nodes.to_file('../data/reg_nodes.gpkg', driver='GPKG')
reg_edges.to_file('../data/red_edges.gpkg', driver='GPKG')

#%%
# Check crs
nodes, edges = ox.graph_to_gdfs(G_sim)

assert edges.crs == crs, 'Data is in wrong crs!'

#%%

# Create unique id
edges['old_id'] = edges.fot_id

edges.reset_index(inplace=True)

ids = []
for i in range(1000, 1000+len(edges)):
    ids.append(i)

edges['new_id'] = ids

assert len(edges.new_id.unique()) == len(edges)

#%%

if use_postgres:

    connection = dbf.connect_pg(db_name, db_user, db_password)

    engine = dbf.connect_alc(db_name, db_user, db_password, db_port=db_port)

    dbf.to_postgis(geodataframe=geodk, table_name='vm_brudt', engine=engine)


    q = 'SELECT fot_id, feat_type FROM vm_brudt LIMIT 10;'

    test = dbf.run_query_pg(q, connection)

    print(test)

    connection.close()

else:
 
    with open('../data/reference_data.pickle', 'wb') as handle:
        pickle.dump(geodk, handle, protocol=pickle.HIGHEST_PROTOCOL)

# %%
