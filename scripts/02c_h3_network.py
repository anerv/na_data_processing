'''
Index network elements by h3 index

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

#%%
# Reproject to WGS84
osm_nodes.to_crs('EPSG:4326',inplace=True)
osm_edges.to_crs('EPSG:4326',inplace=True)
#%%
# INDEX ALL NODES AT VARIOUS H3 LEVELS

# index each data point into the spatial index of the specified resolution
for res in range(7, 11):
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


#%%
# INDEX EDGES AT VARIOUS H3 LEVELS - or just at one? E.g. 9
h3_res_level = 9

def coords_to_h3(coords, h3_res):

    h3_indices_set = set()

    for c in coords:
        # Index point to h3 at h3_res
        index = h3.geo_to_h3(lat=c[1],lng=c[0], resolution = h3_res)
        # Add index to set
        h3_indices_set.add(index)

    h3_indices = list(h3_indices_set)

    return h3_indices

def h3_fill_line():

    # Function for filling out h3 cells between non-adjacent cells

    pass

def h3_index_to_geometry(h3_indices, shapely_polys=False):

    polygon_coords = []

    for h in h3_indices:
        
        h3_coords = h3.h3_to_geo_boundary(h=h, geo_json=True)
        polygon_coords.append(h3_coords)

    if shapely_polys:
        
        polys = [Polygon(p) for p in polygon_coords]

        return polys

    return polygon_coords


# Create column with edge coordinates
osm_edges['coords'] = osm_edges['geometry'].apply(lambda x: gf.return_coord_list(x))
osm_edges[f'h3_index_{h3_res_level}'] = osm_edges['coords'].apply(lambda x: coords_to_h3(x,h3_res_level))

#%%
coords = gf.return_coord_list(osm_edges.loc[41504,'geometry'])

h3_index = coords_to_h3(coords,13)

polys = h3_index_to_geometry(h3_index, shapely_polys=True)

gdf = gpd.GeoDataFrame(geometry=polys,crs=osm_edges.crs)
# %%
fig, ax = plt.subplots()
gdf.plot(ax=ax)
osm_edges.loc[osm_edges.edge_id==110372].plot(ax=ax,color='red')
# %%
