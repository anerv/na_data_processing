'''
The purpose of this script is to
1) Convert OSM data to a graph format. This step is using pyrosm which has been optimized for loading OSM data from pbf files for large areas
2) Load the resulting data to a PostGIS database 

'''
#%%
import pyrosm
import psycopg2 as pg
import geopandas as gpd
import yaml
import matplotlib.pyplot as plt
import osmnx as ox
import networkx as nx
from src import db_functions as dbf
#%%

with open(r'config.yml') as file:
    parsed_yaml_file = yaml.load(file, Loader=yaml.FullLoader)


    study_area = parsed_yaml_file['study_area']
    osm_fp = parsed_yaml_file['OSM_fp']

    db_name = parsed_yaml_file['db_name']
    db_user = parsed_yaml_file['db_user']
    db_password = parsed_yaml_file['db_password']
    db_host = parsed_yaml_file['db_host']
    db_port = parsed_yaml_file['db_port']
  
    
print('Settings loaded!')


#%%
'''
Load OSM data network data, convert to graph format, get nodes and edges, load to db
'''
osm = pyrosm.OSM(osm_fp)

nodes, edges = osm.get_network(nodes=True)

G = osm.to_graph(nodes, edges, graph_type="networkx")

ox_nodes, ox_edges = ox.graph_to_gdfs(G)

#%%
connection = dbf.connect_pg(db_name, db_user, db_password)

engine_test = dbf.connect_alc(db_name, db_user, db_password, db_port=db_port)

# Create tables?
# Load with to postgis

# Check result?

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