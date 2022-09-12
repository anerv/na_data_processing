'''
Index network elements by h3 index

# TODO: Return h3 index at level X for all edge ids and node ids
# TODO: Export

Method for getting all h3 cells at given level on the same component

Method for finding distance between two H3 cells
Method for determining whether two cells are connected
'''
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

with open(r'../config.yml') as file:

    parsed_yaml_file = yaml.load(file, Loader=yaml.FullLoader)

    crs = parsed_yaml_file['CRS']

    db_name = parsed_yaml_file['db_name']
    db_user = parsed_yaml_file['db_user']
    db_password = parsed_yaml_file['db_password']
    db_host = parsed_yaml_file['db_host']
    db_port = parsed_yaml_file['db_port']
  
print('Settings loaded!')

#%%
# Load data
engine = dbf.connect_alc(db_name, db_user, db_password, db_port=db_port)

get_osm_edges = 'SELECT highway, edge_id, geometry FROM cycling_edges;'
get_osm_nodes = 'SELECT osmid, geometry FROM cycling_nodes;'

osm_edges = gpd.GeoDataFrame.from_postgis(get_osm_edges, engine, geom_col='geometry')
osm_nodes = gpd.GeoDataFrame.from_postgis(get_osm_nodes, engine, geom_col='geometry')

osm_segments = mf.create_segment_gdf(osm_edges,10)
#%%
# Reproject to WGS84
osm_nodes.to_crs('EPSG:4326',inplace=True)
osm_edges.to_crs('EPSG:4326',inplace=True)
osm_segments.to_crs('EPSG:4326',inplace=True)
#%%

def coords_to_h3(coords, h3_res):

    h3_indices_set = set()

    for c in coords:
        # Index point to h3 at h3_res
        index = h3.geo_to_h3(lat=c[1],lng=c[0], resolution = h3_res)
        # Add index to set
        h3_indices_set.add(index)

    h3_indices = list(h3_indices_set)

    # if len(h3_indices) == 1:

    #     return h3_indices[0]

    return h3_indices

def h3_index_to_geometry(h3_indices, shapely_polys=False):

    polygon_coords = []

    for h in h3_indices:
        
        h3_coords = h3.h3_to_geo_boundary(h=h, geo_json=True)
        polygon_coords.append(h3_coords)

    if shapely_polys:
        
        polys = [Polygon(p) for p in polygon_coords]

        return polys

    return polygon_coords


def h3_fill_line(h3_edge_indices):

    # Function for filling out h3 cells between non-adjacent cells

    if len(h3_edge_indices) < 2:

        return h3_edge_indices

    h3_line = set()

    h3_line.update(h3_edge_indices)

    for i in range(0,len(h3_edge_indices)-1):

        if h3.h3_indexes_are_neighbors(h3_edge_indices[i],h3_edge_indices[i+1]) == False:

            missing_hexs = h3.h3_line(h3_edge_indices[i],h3_edge_indices[i+1])

            h3_line.update(missing_hexs)

    return list(h3_line)

#%%
# INDEX EDGES AT VARIOUS H3 LEVELS - or just at one? E.g. 9
h3_res_level = 11
#%%
# Create column with edge coordinates
osm_segments['coords'] = osm_segments['geometry'].apply(lambda x: gf.return_coord_list(x))
osm_segments[f'h3_index_{h3_res_level}'] = osm_segments['coords'].apply(lambda x: coords_to_h3(x,h3_res_level))
osm_segments[f'filled_h3_index_{h3_res_level}'] = osm_segments[f'h3_index_{h3_res_level}'].apply(lambda x: h3_fill_line(x))

#%%
# PLOT EDGE DENSITY

index_list = osm_segments[f'filled_h3_index_{h3_res_level}'].values
test = list(itertools.chain(*index_list))
counts = dict(Counter(test))

edge_df = pd.DataFrame.from_dict(counts,orient='index').reset_index()
hex_id_col = f'h3_index_{h3_res_level}'
edge_df.rename({'index': hex_id_col,0:'count'},axis=1,inplace=True)

edge_df['lat'] = edge_df[hex_id_col].apply(lambda x: h3.h3_to_geo(x)[0])
edge_df['long'] = edge_df[hex_id_col].apply(lambda x: h3.h3_to_geo(x)[1])

edge_df.plot.scatter(x='long',y='lat',c='count',marker='o',edgecolors='none',colormap='viridis',figsize=(30,20))
plt.xticks([], []); plt.yticks([], []);

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
                                                    lat = row['geometry'].y,
                                                    lng = row['geometry'].x,
                                                    resolution = res),
                                        axis = 1)

    # use h3.h3_to_geo_boundary to obtain the geometries of these hexagons
    osm_nodes[col_geom] = osm_nodes[col_hex_id].apply(
                                        lambda x: {"type": "Polygon",
                                                   "coordinates":
                                                   [h3.h3_to_geo_boundary(
                                                       h=x, geo_json=True)]
                                                   }
                                        )      

#%%                          
# PLOT NODE DENSITY
osm_nodes.plot()
hex_id_col = 'hex_id_10'
grouped = osm_nodes.groupby(hex_id_col).size().to_frame('count').reset_index()

grouped['lat'] = grouped[hex_id_col].apply(lambda x: h3.h3_to_geo(x)[0])
grouped['long'] = grouped[hex_id_col].apply(lambda x: h3.h3_to_geo(x)[1])

grouped.plot.scatter(x='long',y='lat',c='count',marker='o',edgecolors='none',colormap='viridis',figsize=(30,20))
plt.xticks([], []); plt.yticks([], []);
# %%
