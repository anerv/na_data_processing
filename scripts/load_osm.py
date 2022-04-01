'''
The purpose of this script is to
1) Convert OSM data to a graph format. This step is using pyrosm which has been optimized for loading OSM data from pbf files for large areas
2) Load the resulting data to a PostGIS database or alternatively to disk file.

Requires a postgres db with postgis extension activated if the postgres option is used!

'''

# TODO: remove unused columns from osm data, load other osm data (e.g. traffic lights)

#%%
import pyrosm
import yaml
import osmnx as ox
from src import db_functions as dbf
import pickle
from src import simplification_functions as sf
from src import matching_functions as mf
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
osm = pyrosm.OSM(osm_fp)

extra_attr = ['cycleway:left','cycleway:right','cycleway:both','cycleway:width',
            'cycleway:left:width','cycleway:right:width','cycleway:both:width','bicycle_road','oneway_bicycle'
            'cycleway:surface','cyclestreet','sidewalk','crossing','barrier','bollard','flashing_lights','proposed','construction']

nodes, edges = osm.get_network(nodes=True, network_type='all', extra_attributes=extra_attr)

G = osm.to_graph(nodes, edges, graph_type="networkx", retain_all=True)

#%%
# Convert to index format used by osmnx
ox_nodes, ox_edges = ox.graph_to_gdfs(G)

ox_edges = mf.clean_col_names(ox_edges)
ox_nodes = mf.clean_col_names(ox_nodes)

#%%
# Add attribute on whether cycling infra exist or not (to be used by e.g. simplification function)
ox_edges['cycling_infra'] = 'no'

queries = ["highway == 'cycleway'",
        "highway == 'living_street'",
        "cycleway in ['lane','track','opposite_lane','opposite_track','shared_lane','designated','crossing','share_busway']",
        "cycleway_left in ['lane','track','opposite_lane','opposite_track','shared_lane','designated','crossing','share_busway']",
        "cycleway_right in ['lane','track','opposite_lane','opposite_track','shared_lane','designated','crossing','share_busway']",
        "cycleway_both in ['lane','track','opposite_lane','opposite_track','shared_lane','designated','crossing','share_busway']",
        "bicycle_road == 'yes'",
        "highway == 'track' & bicycle in ['designated','yes']",
        "highway == 'service' & (bicycle == 'designated' or motor_vehicle == 'no')",
        "highway == 'path' & bicycle in ['designated','yes']"
        ]

for q in queries:
    ox_filtered = ox_edges.query(q)

    ox_edges.loc[ox_filtered.index, 'cycling_infra'] = 'yes'

ox_edges.cycling_infra.value_counts()
#%%
# TODO: Filter out edges with irrelevant highway types
unused_highway_values = ['proposed','construction','disused','elevator','platform','bus_stop','step','steps','corridor','raceway']

ox_edges.loc[ox_edges.highway not in unused_highway_values]
#%%  
# Recreate graph with new attribute to simplify 
G_updated = ox.graph_from_gdfs(ox_nodes, ox_edges) # type is MultiDiGraph

G_sim = sf.simplify_graph(G_updated, attributes = ['cycling_infra','highway'])

# TODO: Consolidate intersections? 

# Get undirected now
G_sim_un = ox.get_undirected(G_sim)

# Project to project crs
G_sim_un = ox.project_graph(G_sim_un, to_crs=crs)

# Get simplified ox_edges and nodes to be used in matching process
ox_nodes_s, ox_edges_s = ox.graph_to_gdfs(G_sim_un)

#%%
# Create unique id (due to the way the network is created, several edges can have the same osm id)
ox_edges_s['first_osmid'] = ox_edges_s.osmid

ox_edges_s.reset_index(inplace=True)

ids = []
for i in range(1000, 1000+len(ox_edges_s)):
    ids.append(i)

ox_edges_s.osmid = ids

assert len(ox_edges_s.osmid.unique()) == len(ox_edges_s)

assert ox_edges_s.crs == crs, 'Data is in wrong crs!'

#%%
# Export data
if use_postgres:

    print('Saving data to PostgreSQL!')

    ox_nodes_s.reset_index(inplace=True, drop=True)

    connection = dbf.connect_pg(db_name, db_user, db_password)

    engine = dbf.connect_alc(db_name, db_user, db_password, db_port=db_port)

    dbf.to_postgis(geodataframe=ox_edges_s, table_name='osm_edges', engine=engine)

    dbf.to_postgis(ox_nodes_s, 'osm_nodes', engine)

    q = 'SELECT osmid, name, highway FROM osm_edges LIMIT 10;'

    test = dbf.run_query_pg(q, connection)

    print(test)

    connection.close()

else:

    print('Saving data to file!')

    ox.save_graphml(G_sim_un, filepath='../data/graph_osm.graphml')

    with open('../data/osm_edges_sim.pickle', 'wb') as handle:
        pickle.dump(ox_edges_s, handle, protocol=pickle.HIGHEST_PROTOCOL)

    with open('../data/osm_nodes_sim.pickle', 'wb') as handle:
        pickle.dump(ox_nodes_s, handle, protocol=pickle.HIGHEST_PROTOCOL)
#%%
# Load other data from OSM? E.g. traffic lights
