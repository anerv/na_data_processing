"""
The purpose of this script is to convert OSM data to a graph format, drop unnecessary data and simplify the network. 
The script is using pyrosm which has been optimized for loading OSM data from pbf files for large areas.
"""

import pyrosm
import yaml
import osmnx as ox
import networkx as nx
import pandas as pd
import geopandas as gpd
import json
import pickle
from src import simplification_functions as sf
from src import graph_functions as gf
from timeit import default_timer as timer
import os.path

with open(r"../config.yml") as file:

    parsed_yaml_file = yaml.load(file, Loader=yaml.FullLoader)

    osm_fp = parsed_yaml_file["osm_fp"]

    ref_id_col = parsed_yaml_file["geodk_id_col"]

    crs = parsed_yaml_file["CRS"]

    db_name = parsed_yaml_file["db_name"]
    db_user = parsed_yaml_file["db_user"]
    db_password = parsed_yaml_file["db_password"]
    db_host = parsed_yaml_file["db_host"]
    db_port = parsed_yaml_file["db_port"]

print("Settings loaded!")

osm = pyrosm.OSM(osm_fp)

extra_attr = [
    "surface",
    "cycleway:left",
    "cycleway:right",
    "cycleway:both",
    "cycleway:width",
    "pedestrian:surface",
    "cycleway:left:width",
    "cycleway:right:width",
    "cycleway:both:width",
    "bicycle_road",
    "oneway_bicycle",
    "maxspeed",
    "cycleway:surface",
    "cyclestreet",
    "sidewalk",
    "crossing",
    "barrier",
    "bollard",
    "flashing_lights",
    "proposed",
    "construction",
]


print("Creating edge and node datasets...")
nodes, edges = osm.get_network(
    nodes=True, network_type="all", extra_attributes=extra_attr
)

# # Explode tags dict to get cycleway surface
# edges['temp_id'] = edges.index

# edges_selection = edges.loc[edges.tags.notna()].copy()

# edges_selection['dict'] = edges_selection['tags'].astype(str).apply(lambda x: json.loads(x))
# edges_selection = edges_selection[['temp_id','dict']]

# expl = edges_selection.dict.apply(pd.Series)
# expl = expl[['cycleway:surface','footway:surface']]
# attr = expl.join(edges_selection)

# edges = edges.merge(attr, on='temp_id', how='left')
# edges.drop('dict',axis=1,inplace=True)
# edges.drop('temp_id', axis=1, inplace=True)
# edges.rename({'dict':'tags'},inplace=True, axis=1)

# Filter out edges with irrelevant highway types
unused_highway_values = [
    "abandoned",
    "planned",
    "proposed",
    "construction",
    "disused",
    "elevator",
    "platform",
    "bus_stop",
    "step",
    "steps",
    "corridor",
    "raceway",
    "bus_guideway",
    "rest_area",
    "razed",
    "layby",
    "skyway",
    "su",
]

org_len = len(edges)
edges = edges.loc[~edges.highway.isin(unused_highway_values)]

# Filter out pedestrian edges
pedestrian_edges = edges.loc[
    (edges.highway.isin(["footway", "pedestrian"]))
    & (
        ~edges.bicycle.isin(
            ["allowed", "ok", "designated", "permissive", "yes", "destination"]
        )
    )
]
edges.drop(pedestrian_edges.index, inplace=True)

new_len = len(edges)

print(f"{org_len - new_len} edges where removed")

# Filter unused nodes
node_id_list = list(set(edges.u.to_list() + edges.v.to_list()))
nodes = nodes.loc[nodes.id.isin(node_id_list)]

# Drop unnecessary cols
edges.drop(
    ["overtaking", "psv", "ref", "int_ref", "construction", "proposed"],
    axis=1,
    inplace=True,
)

# Create networkx graph
# start = timer()

G = osm.to_graph(nodes, edges, graph_type="networkx", retain_all=True)

# end = timer()
# print(end - start)


# Save graph
with open("../data/osm_pyrosm_graph", "wb") as handle:
    pickle.dump(G, handle, protocol=pickle.HIGHEST_PROTOCOL)

# Fill na/None values for simplification
gf.update_edge_data(G, "cycleway", None, "unknown")
gf.update_edge_data(G, "cycleway:right", None, "unknown")
gf.update_edge_data(G, "cycleway:left", None, "unknown")
gf.update_edge_data(G, "cycleway:both", None, "unknown")
gf.update_edge_data(G, "cycleway_surface", None, "unknown")
gf.update_edge_data(G, "bicycle_road", None, "unknown")
gf.update_edge_data(G, "maxspeed", None, "unknown")
gf.update_edge_data(G, "lit", None, "unknown")
gf.update_edge_data(G, "surface", None, "unknown")
gf.update_edge_data(G, "bicycle", None, "unknown")
gf.update_edge_data(G, "cyclestreet", None, "unknown")
gf.update_edge_data(G, "access", None, "unknown")

# Convert to index format used by osmnx
ox_nodes, ox_edges = ox.graph_to_gdfs(G)
# Remove geometry attribute (required by simplification function)
ox_edges.drop("geometry", axis=1, inplace=True)
G_ox = ox.graph_from_gdfs(ox_nodes, ox_edges)


# Simplify grap
G_sim = sf.simplify_graph(
    G_ox,
    attributes=[
        "highway",
        "cycleway",
        "cycleway:right",
        "cycleway:left",
        "cycleway:both",
        "bicycle_road",
        "maxspeed",
        "lit",
        "surface",
        "cycleway_surface",
        "bicycle",
        "cyclestreet",
        "access",
    ],
)

# Get undirected
G_sim_un = ox.get_undirected(G_sim)
G_un = ox.get_undirected(G)

# Project to project crs
G_sim_un = ox.project_graph(G_sim_un, to_crs=crs)
G_un = ox.project_graph(G_un, to_crs=crs)

# Get simplified and undirected ox_edges and nodes
ox_nodes_s, ox_edges_s = ox.graph_to_gdfs(G_sim_un)
ox_nodes, ox_edges = ox.graph_to_gdfs(G_un)

# Create unique id (due to the way the network is created, several edges can have the same osm id)
ox_edges_s["org_osmid"] = ox_edges_s.osmid

ox_edges_s.reset_index(inplace=True)

ox_edges["org_osmid"] = ox_edges.osmid

ox_edges.reset_index(inplace=True)

ox_edges["edge_id"] = ox_edges.reset_index().index
ox_edges_s["edge_id"] = ox_edges_s.reset_index().index

assert len(ox_edges_s["edge_id"].unique()) == len(ox_edges_s)
assert len(ox_edges["edge_id"].unique()) == len(ox_edges)

assert ox_edges_s.crs == crs, "Data is in wrong crs!"
assert ox_edges.crs == crs, "Data is in wrong crs!"

# Export data

ox_edges = gf.clean_col_names(ox_edges)
ox_nodes = gf.clean_col_names(ox_nodes)
ox_edges_s = gf.clean_col_names(ox_edges_s)
ox_nodes_s = gf.clean_col_names(ox_nodes_s)

# print('Saving data to PostgreSQL!')

# ox_nodes_s.reset_index(inplace=True, drop=True)

# connection = dbf.connect_pg(db_name, db_user, db_password)

# engine = dbf.connect_alc(db_name, db_user, db_password, db_port=db_port)

# dbf.to_postgis(geodataframe=ox_edges, table_name='osm_edges', engine=engine)

# dbf.to_postgis(ox_nodes, 'osm_nodes', engine)

# dbf.to_postgis(geodataframe=ox_edges_s, table_name='osm_edges_simplified', engine=engine)

# dbf.to_postgis(ox_nodes_s, 'osm_nodes_simplified', engine)

# q = 'SELECT edge_id, name, highway FROM osm_edges_simplified LIMIT 10;'

# test = dbf.run_query_pg(q, connection)

# print(test)

# connection.close()


# # Classify cycling infrastructure

# connection = dbf.connect_pg(db_name, db_user, db_password)

# q = 'sql/define_cycling_infra_osm.sql'

# cycling_infra = dbf.run_query_pg(q, connection)

# q = "SELECT name, highway, cycling_infrastructure from osm_edges_simplified WHERE cycling_infrastructure = 'yes' LIMIT 10;"

# test = dbf.run_query_pg(q, connection)

# print(test)

# connection.close()

# print('Saving data to file!')

ox.save_graphml(G_sim_un, filepath="../data/graph_osm_simple.graphml")
ox.save_graphml(G_un, filepath="../data/graph_osm.graphml")

with open("../data/osm_edges.pickle", "wb") as handle:
    pickle.dump(ox_edges, handle, protocol=pickle.HIGHEST_PROTOCOL)

with open("../data/osm_nodes.pickle", "wb") as handle:
    pickle.dump(ox_nodes, handle, protocol=pickle.HIGHEST_PROTOCOL)

with open("../data/osm_edges_sim.pickle", "wb") as handle:
    pickle.dump(ox_edges_s, handle, protocol=pickle.HIGHEST_PROTOCOL)

with open("../data/osm_nodes_sim.pickle", "wb") as handle:
    pickle.dump(ox_nodes_s, handle, protocol=pickle.HIGHEST_PROTOCOL)
