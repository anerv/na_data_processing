'''
The purpose of this script is to
1) Convert OSM data to a graph format. This step is using pyrosm which has been optimized for loading OSM data from pbf files for large areas
2) Load the resulting data to a PostGIS database or alternatively to disk file.

Requires a postgres db with postgis extension activated if the postgres option is used!
'''

#%%
import pyrosm
import yaml
import osmnx as ox
import networkx as nx
import pandas as pd
import json
from src import db_functions as dbf
import pickle
from src import simplification_functions as sf
from src import graph_functions as gf
from timeit import default_timer as timer
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

extra_attr = ['cycleway:left','cycleway:right','cycleway:both','cycleway:width',
            'cycleway:left:width','cycleway:right:width','cycleway:both:width','bicycle_road','oneway_bicycle'
            'cycleway:surface','cyclestreet','sidewalk','crossing','barrier','bollard','flashing_lights','proposed','construction']

#%%
nodes, edges = osm.get_network(nodes=True, network_type='all', extra_attributes=extra_attr)
#%%
with open('../data/osm_edges_org', 'wb') as handle:
    pickle.dump(edges, handle, protocol=pickle.HIGHEST_PROTOCOL)

with open('../data/osm_nodes_org.pickle', 'wb') as handle:
    pickle.dump(nodes, handle, protocol=pickle.HIGHEST_PROTOCOL)

#%%
# Filter out edges with irrelevant highway types
unused_highway_values = ['abandoned','planned','proposed','construction','disused','elevator',
                        'platform','bus_stop','step','steps','corridor',
                        'raceway','bus_guideway','rest_area','razed','layby','skyway','su']

org_len = len(edges)
edges = edges.loc[~edges.highway.isin(unused_highway_values)]
new_len = len(edges)

print(f'{org_len - new_len} edges where removed')

#%%
# Create subselection of OSM edges in specific area, to test!
xmin = 12.240496
ymin = 55.617982
xmax = 12.591587
ymax = 55.750171

edges_subset = edges.cx[xmin:xmax, ymin:ymax]

node_subset_list = list(set(edges_subset.u.to_list() + edges_subset.v.to_list()))
nodes_subset = nodes.loc[nodes.id.isin(node_subset_list)]
#%%
# Create networkx graph
start = timer()

G = osm.to_graph(nodes_subset, edges_subset, graph_type="networkx", retain_all=True)

end = timer()
print(end - start)

#%%
# Convert to index format used by osmnx
ox_nodes, ox_edges = ox.graph_to_gdfs(G)

#%%
G_updated = G.edge_subgraph(ox_edges.index)

#%%
# Update edges based on matches with reference data
matches_fp = open('../data/matches.json')

matches = json.load(matches_fp)
  
final_matches_df = pd.DataFrame.from_dict(matches,orient='index')
final_matches_df.rename(columns={0:'way_id'},inplace=True)
final_matches_df.reset_index(inplace=True)
final_matches_df.rename({'index':ref_id_col},inplace=True,axis=1)

updated_osm = ox_edges.merge(final_matches_df, left_on='osmid',right_on='way_id',how='left')

#%%
# Add attribute on whether cycling infra exist or not (to be used by e.g. simplification function)
ox_edges = gf.clean_col_names(ox_edges)
ox_nodes = gf.clean_col_names(ox_nodes)

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
        "highway == 'path' & bicycle in ['designated','yes']",
        f"{ref_id_col}.notnull()"
        ]

for q in queries:
    ox_filtered = ox_edges.query(q)

    ox_edges.loc[ox_filtered.index, 'cycling_infra'] = 'yes'

ox_edges.cycling_infra.value_counts()

# Save data to graph
cycling_infra_dict = ox_edges['cycling_infrastructure'].to_dict()
nx.set_edge_attributes(G, cycling_infra_dict, 'cycling_infrastructure')

matches_id_dict = ox_edges[ref_id_col].to_dict()
nx.set_edge_attributes(G, matches_id_dict, 'matches_id')
#%%
# Simplify grap
G_sim = sf.simplify_graph(G_updated, attributes = ['cycling_infra','highway'])

# Get undirected
G_sim_un = ox.get_undirected(G_sim)

G_un = ox.get_undirected(G_updated)

# Project to project crs
G_sim_un = ox.project_graph(G_sim_un, to_crs=crs)
G_un = ox.project_graph(G_un, to_crs=crs)

# Get simplified and undirected ox_edges and nodes
ox_nodes_s, ox_edges_s = ox.graph_to_gdfs(G_sim_un)
ox_nodes, ox_edges = ox.graph_to_gdfs(G_un)

#%%
# Create unique id (due to the way the network is created, several edges can have the same osm id)
ox_edges_s['org_osmid'] = ox_edges_s.osmid

ox_edges_s.reset_index(inplace=True)

ids = []
for i in range(1000, 1000+len(ox_edges_s)):
    ids.append(i)

ox_edges_s.osmid = ids

assert len(ox_edges_s.osmid.unique()) == len(ox_edges_s)

assert ox_edges_s.crs == crs, 'Data is in wrong crs!'

# TODO: Assert that also non-simplified edges have unique id

#%%
# Export data




if use_postgres:

    print('Saving data to PostgreSQL!')

    ox_nodes_s.reset_index(inplace=True, drop=True)

    connection = dbf.connect_pg(db_name, db_user, db_password)

    engine = dbf.connect_alc(db_name, db_user, db_password, db_port=db_port)

    dbf.to_postgis(geodataframe=ox_edges, table_name='osm_edges', engine=engine)

    dbf.to_postgis(ox_nodes, 'osm_nodes', engine)

    dbf.to_postgis(geodataframe=ox_edges_s, table_name='osm_edges_simplified', engine=engine)

    dbf.to_postgis(ox_nodes_s, 'osm_nodes_simplified', engine)

    q = 'SELECT osmid, name, highway FROM osm_edges LIMIT 10;'

    test = dbf.run_query_pg(q, connection)

    print(test)

    connection.close()

else:

    print('Saving data to file!')

    ox.save_graphml(G_sim_un, filepath='../data/graph_osm.graphml')
    ox.save_graphml(G_sim_un, filepath='../data/graph_osm.graphml')

    with open('../data/osm_edges_sim.pickle', 'wb') as handle:
        pickle.dump(ox_edges_s, handle, protocol=pickle.HIGHEST_PROTOCOL)

    with open('../data/osm_nodes_sim.pickle', 'wb') as handle:
        pickle.dump(ox_nodes_s, handle, protocol=pickle.HIGHEST_PROTOCOL)
#%%
# Load other data from OSM? E.g. traffic lights

#%%
# TODO: Load traffic lights etc to DB?
