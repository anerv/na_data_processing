'''
The purpose of this script is to
1) Convert OSM data to a graph format. This step is using pyrosm which has been optimized for loading OSM data from pbf files for large areas
2) Load the resulting data to a PostGIS database or alternatively to disk file.

Requires a postgres db with postgis extension activated if the postgres option is used!

'''

# TODO: remove unused columns from osm data, load other osm data (e.g. traffic lights)
# TODO: use simplified version of OSM!!
#%%
import pyrosm
import yaml
import osmnx as ox
from src import db_functions as dbf
import pickle
from src import simplification_functions as sf
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
            'cycleway:left:width','cycleway:right:width','cycleway:both:width','bicycle_road',
            'cycleway:surface','cyclestreet','sidewalk','crossing','barrier','bollard','flashing_lights']

nodes, edges = osm.get_network(nodes=True, network_type='all', extra_attributes=extra_attr)

G = osm.to_graph(nodes, edges, graph_type="networkx", retain_all=True)

G = ox.get_undirected(G)

G = ox.project_graph(G, to_crs=crs)

#%%

ox_nodes, ox_edges = ox.graph_to_gdfs(G)

ox_edges.columns = ox_edges.columns.str.lower()
ox_nodes.columns = ox_nodes.columns.str.lower()

edge_cols = ox_edges.columns.to_list()

new_edge_cols = [c.replace(':','_') for c in edge_cols]

ox_edges.columns = new_edge_cols

node_cols = ox_nodes.columns

new_node_cols = [c.replace(':','_') for c in node_cols]

ox_nodes.columns = new_node_cols

#%%
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

#%%  
# Recreate graph with new attribute to simplify (type is now MultiDiGraph)
graph = ox.graph_from_gdfs(ox_nodes, ox_edges)

#%%
g_sim = sf.simplify_graph(graph, attributes = 'cycling_infra')
#%%
# Recreate ox_edges and nodes to be used in matching process

ox_nodes, ox_edges = ox.graph_to_gdfs(g_sim)

ox_edges.columns = ox_edges.columns.str.lower()
ox_nodes.columns = ox_nodes.columns.str.lower()

edge_cols = ox_edges.columns.to_list()

new_edge_cols = [c.replace(':','_') for c in edge_cols]

ox_edges.columns = new_edge_cols

node_cols = ox_nodes.columns

new_node_cols = [c.replace(':','_') for c in node_cols]

ox_nodes.columns = new_node_cols
#%%
# Create unique osmid col
ox_edges['old_osmid'] = ox_edges.osmid

ox_edges.reset_index(inplace=True)

ids = []
for i in range(1000, 1000+len(ox_edges)):
    ids.append(i)

ox_edges.osmid = ids

assert len(ox_edges.osmid.unique()) == len(ox_edges)

assert ox_edges.crs == crs, 'Data is in wrong crs!'

#%%
if use_postgres:

    print('Saving data to PostgreSQL!')

    ox_nodes.reset_index(inplace=True, drop=True)

    connection = dbf.connect_pg(db_name, db_user, db_password)

    engine = dbf.connect_alc(db_name, db_user, db_password, db_port=db_port)

    dbf.to_postgis(geodataframe=ox_edges, table_name='osm_edges', engine=engine)

    dbf.to_postgis(ox_nodes, 'osm_nodes', engine)

    q = 'SELECT osmid, name, highway FROM osm_edges LIMIT 10;'

    test = dbf.run_query_pg(q, connection)

    print(test)

    connection.close()

else:

    print('Saving data to file!')

    ox.save_graphml(G, filepath='../data/graph_osm.graphml')

    with open('../data/osm_edges.pickle', 'wb') as handle:
        pickle.dump(ox_edges, handle, protocol=pickle.HIGHEST_PROTOCOL)

    with open('../data/osm_nodes.pickle', 'wb') as handle:
        pickle.dump(nodes, handle, protocol=pickle.HIGHEST_PROTOCOL)

#%%
# Load other data from OSM? E.g. traffic lights
