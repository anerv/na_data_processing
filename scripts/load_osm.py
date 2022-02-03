'''
The purpose of this script is to
1) Convert OSM data to a graph format. This step is using pyrosm which has been optimized for loading OSM data from pbf files for large areas
2) Load the resulting data to a PostGIS database 

Requires a postgres db with postgis extension activated!

'''

# TODO: remove unused columns from osm data
#%%
import pyrosm
import yaml
import osmnx as ox
from src import db_functions as dbf
#%%

with open(r'config.yml') as file:
    parsed_yaml_file = yaml.load(file, Loader=yaml.FullLoader)


    study_area = parsed_yaml_file['study_area']
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
osm = pyrosm.OSM(osm_fp)

nodes, edges = osm.get_network(nodes=True)

G = osm.to_graph(nodes, edges, graph_type="networkx")

G = ox.project_graph(G, to_crs=crs)

ox_nodes, ox_edges = ox.graph_to_gdfs(G)

assert ox_edges.crs == crs, 'Data is in wrong crs!'

ox_edges.reset_index(inplace=True)
ox_nodes.reset_index(inplace=True, drop=True)
#%%
connection = dbf.connect_pg(db_name, db_user, db_password)

engine = dbf.connect_alc(db_name, db_user, db_password, db_port=db_port)

#%%
dbf.to_postgis(geodataframe=ox_edges, table_name='osm_edges', engine=engine)

dbf.to_postgis(ox_nodes, 'osm_nodes', engine)

q = 'SELECT osmid, HIGHWAY FROM osm_edges LIMIT 10;'

test = dbf.run_query_pg(q, connection)

print(test)
#%%
# Load other data from OSM? E.g. traffic lights

# %%
'''
osmnx and pandana create the same number of edges (indicating same structure for network)
- but pandana seems to be a simpler df structure.
- osmnx needs the multiindex
- seems to be simpler to create pandana edge list from osmnx than vice versa
- workflow could be to load osmnx data to postgis database - do processing - load back to geodataframe
- and then convert to which graph type I need
'''