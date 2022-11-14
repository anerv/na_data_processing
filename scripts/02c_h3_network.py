"""
Index network elements by h3 index

# TODO Return all H3 indices for a given component - how do you store H3 data best?

"""
#%%
import h3
import geopandas as gpd
import pandas as pd
import yaml
import matplotlib.pyplot as plt
import json
import pickle
from src import db_functions as dbf
from src import graph_functions as gf
from timeit import default_timer as timer
from shapely.geometry import Polygon
from src import matching_functions as mf
import itertools
from collections import Counter
from src import h3_functions as h3_func

with open(r"../config.yml") as file:

    parsed_yaml_file = yaml.load(file, Loader=yaml.FullLoader)

    crs = parsed_yaml_file["CRS"]

    db_name = parsed_yaml_file["db_name"]
    db_user = parsed_yaml_file["db_user"]
    db_password = parsed_yaml_file["db_password"]
    db_host = parsed_yaml_file["db_host"]
    db_port = parsed_yaml_file["db_port"]

print("Settings loaded!")

#%%
# Load data
engine = dbf.connect_alc(db_name, db_user, db_password, db_port=db_port)

get_osm_edges = "SELECT highway, edge_id, geometry FROM cycling_edges;"
get_osm_nodes = "SELECT osmid, geometry FROM cycling_nodes;"

osm_edges = gpd.GeoDataFrame.from_postgis(get_osm_edges, engine, geom_col="geometry")
osm_nodes = gpd.GeoDataFrame.from_postgis(get_osm_nodes, engine, geom_col="geometry")

#%%
# Segmentize for edge indexing
osm_segments = mf.create_segment_gdf(osm_edges, 10)
#%%
# Reproject to WGS84
osm_nodes.to_crs("EPSG:4326", inplace=True)
osm_edges.to_crs("EPSG:4326", inplace=True)
osm_segments.to_crs("EPSG:4326", inplace=True)
#%%

# TODO: SPLIT INTO LTS SUB-NETWORKS
#%%
# INDEX EDGES
h3_res_level = 13
hex_id_col = f"h3_index_{h3_res_level}"
filled_hex_id_col = f"filled_h3_index_{h3_res_level}"

# Create column with edge coordinates
osm_segments["coords"] = osm_segments["geometry"].apply(
    lambda x: gf.return_coord_list(x)
)
osm_segments[hex_id_col] = osm_segments["coords"].apply(
    lambda x: h3_func.coords_to_h3(x, h3_res_level)
)
osm_segments[filled_hex_id_col] = osm_segments[hex_id_col].apply(
    lambda x: h3_func.h3_fill_line(x)
)

#%%
# Get h3 indices for edges

edge_h3_results = {}

grouped_segs = osm_segments.groupby("edge_id")
grouped_segs.apply(
    lambda x: h3_func.return_edge_h3_indices(x, filled_hex_id_col, edge_h3_results)
)

assert len(grouped_segs) == len(edge_h3_results)

#%%
# Create polygon geometries
index_list = osm_segments[f"filled_h3_index_{h3_res_level}"].values
unpacked_index_list = list(itertools.chain(*index_list))

h3_df = pd.DataFrame(data=unpacked_index_list, columns=[filled_hex_id_col])

h3_df["hex_geom"] = h3_df[filled_hex_id_col].apply(
    lambda x: {
        "type": "Polygon",
        "coordinates": [h3.h3_to_geo_boundary(h=x, geo_json=True)],
    }
)

h3_df["geometry"] = h3_df["hex_geom"].apply(
    lambda x: Polygon(list(x["coordinates"][0]))
)

h3_gdf = gpd.GeoDataFrame(h3_df, geometry="geometry", crs="EPSG:4326")

#%%
# PLOT EDGE DENSITY (as segment count)
index_list = osm_segments[filled_hex_id_col].values
unpacked_index_list = list(itertools.chain(*index_list))
counts = dict(Counter(unpacked_index_list))

edge_df = pd.DataFrame.from_dict(counts, orient="index").reset_index()
edge_df.rename({"index": hex_id_col, 0: "count"}, axis=1, inplace=True)

edge_df["lat"] = edge_df[hex_id_col].apply(lambda x: h3.h3_to_geo(x)[0])
edge_df["long"] = edge_df[hex_id_col].apply(lambda x: h3.h3_to_geo(x)[1])

edge_df.plot.scatter(
    x="long",
    y="lat",
    c="count",
    marker="o",
    s=200,
    edgecolors="none",
    colormap="viridis",
    figsize=(30, 20),
)
plt.xticks([], [])
plt.yticks([], [])

#%%
# INDEX ALL NODES AT VARIOUS H3 LEVELS

# index each data point into the spatial index of the specified resolution
for res in range(7, 13):
    col_hex_id = "hex_id_{}".format(res)
    col_geom = "geometry_{}".format(res)
    msg_ = "At resolution {} -->  H3 cell id : {} and its geometry: {} "
    print(msg_.format(res, col_hex_id, col_geom))

    osm_nodes[col_hex_id] = osm_nodes.apply(
        lambda row: h3.geo_to_h3(
            lat=row["geometry"].y, lng=row["geometry"].x, resolution=res
        ),
        axis=1,
    )

    # use h3.h3_to_geo_boundary to obtain the geometries of these hexagons
    osm_nodes[col_geom] = osm_nodes[col_hex_id].apply(
        lambda x: {
            "type": "Polygon",
            "coordinates": [h3.h3_to_geo_boundary(h=x, geo_json=True)],
        }
    )

#%%
# PLOT NODE DENSITY
osm_nodes.plot()
hex_id_col = "hex_id_10"
grouped = osm_nodes.groupby(hex_id_col).size().to_frame("count").reset_index()

grouped["lat"] = grouped[hex_id_col].apply(lambda x: h3.h3_to_geo(x)[0])
grouped["long"] = grouped[hex_id_col].apply(lambda x: h3.h3_to_geo(x)[1])

grouped.plot.scatter(
    x="long",
    y="lat",
    c="count",
    marker="o",
    edgecolors="none",
    colormap="viridis",
    figsize=(30, 20),
)
plt.xticks([], [])
plt.yticks([], [])
# %%
# TODO: Export h3 indices for edges/lts sub-networs and nodes
