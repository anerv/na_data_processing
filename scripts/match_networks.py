# Script for matching road networks

# TODO: Functionality for running analysis using a grid

# TODO: Look into where dask-geopandas can be used

#%%
import pickle
import geopandas as gpd
import pandas as pd
import yaml
from src import db_functions as dbf
from src import matching_functions as mf
import osmnx as ox
import matplotlib.pyplot as plt
import os.path
import dask_geopandas as dgpd
import numpy as np
from collections import ChainMap
#%%
with open(r'config.yml') as file:
    parsed_yaml_file = yaml.load(file, Loader=yaml.FullLoader)

    use_postgres = parsed_yaml_file['use_postgres']
    
    crs = parsed_yaml_file['CRS']

    db_name = parsed_yaml_file['db_name']
    db_user = parsed_yaml_file['db_user']
    db_password = parsed_yaml_file['db_password']
    db_host = parsed_yaml_file['db_host']
    db_port = parsed_yaml_file['db_port']

    quality_check = parsed_yaml_file['quality_check']
    quality_data = parsed_yaml_file['quality_data']

    org_ref_id_col = parsed_yaml_file['org_ref_id_col']
  
print('Settings loaded!')

#%%
if use_postgres:
    
    print('Connecting to DB!')
    connection = dbf.connect_pg(db_name, db_user, db_password)

    get_osm = '''SELECT * FROM osm_edges WHERE highway IN ('residential', 'service', 'primary', 'tertiary',
        'tertiary_link', 'secondary', 'cycleway', 'path', 'living_street',
        'unclassified', 'primary_link', 'motorway_link', 'motorway', 'track',
        'secondary_link', 'pathway', 'trunk_link', 'trunk');'''

    get_osm_nodes = 'SELECT * FROM osm_nodes;'

    get_geodk = 'SELECT * FROM geodk_bike;'

    get_grid = 'SELECT * FROM grid;'

    osm_edges = gpd.GeoDataFrame.from_postgis(get_osm, connection, geom_col='geometry' )

    osm_nodes = gpd.GeoDataFrame.from_postgis(get_osm_nodes, geom_col='geometry') #TODO: Only keep nodes used by filtered osm_edges

    reference_data = gpd.GeoDataFrame.from_postgis(get_geodk, connection, geom_col='geometry' )

    grid = gpd.GeoDataFrame.from_postgis(get_grid, connection, geom_col='geometry')

else:

    print('Loading files!')
  
    with open('../data/osm_edges_sim.pickle', 'rb') as fp:
        osm_edges = pickle.load(fp)

    with open('../data/reference_data.pickle', 'rb') as fp:
        reference_data = pickle.load(fp)

    with open('../data/osm_nodes_sim.pickle', 'rb') as fp:
        osm_nodes = pickle.load(fp)

    # TODO: Include paths??
    highway_values = ['residential', 'service', 'primary', 'tertiary',
            'tertiary_link', 'secondary', 'cycleway', 'path', 'living_street',
            'unclassified', 'primary_link', 'motorway_link', 'motorway', 'track',
            'secondary_link', 'pathway', 'trunk_link', 'trunk'
            ]

    osm_edges = osm_edges.loc[osm_edges['highway'].isin(highway_values)]

    reference_data = reference_data.loc[reference_data['vejklasse'].isin(['Cykelbane langs vej', 'Cykelsti langs vej'])]


assert osm_edges.crs == crs
assert reference_data.crs == crs

print(f'Number of rows in osm_edge table: {len(osm_edges)}')
print(f'Number of rows in reference_data table: {len(reference_data)}')

#%%
# Read/create segment data
ref_seg_fp = '../data/ref_segments_full.pickle'
if os.path.exists(ref_seg_fp):
    with open(ref_seg_fp, 'rb') as fp:
        ref_segments = pickle.load(fp)
        #ref_segments.dropna(subset=['geometry'],inplace=True)

else:
    # Create segments
    ref_segments = mf.create_segment_gdf(reference_data, segment_length=10)
    ref_segments.set_crs(crs, inplace=True)
    ref_segments.dropna(subset=['geometry'],inplace=True)

    with open(ref_seg_fp, 'wb') as handle:
        pickle.dump(ref_segments, handle, protocol=pickle.HIGHEST_PROTOCOL)

osm_seg_fp = '../data/osm_segments_full.pickle'
if os.path.exists(osm_seg_fp):
    with open(osm_seg_fp, 'rb') as fp:
        osm_segments = pickle.load(fp)
        osm_segments.dropna(subset=['geometry'],inplace=True)

else:
    osm_segments = mf.create_segment_gdf(osm_edges, segment_length=10)
    osm_segments.rename(columns={'osmid':'org_osmid'}, inplace=True)
    osm_segments.rename(columns={'seg_id':'osmid'}, inplace=True) # Because function assumes an id column names osmid
    osm_segments.set_crs(crs, inplace=True)
    #osm_segments.dropna(subset=['geometry'],inplace=True)

    with open(osm_seg_fp, 'wb') as handle:
        pickle.dump(osm_segments, handle, protocol=pickle.HIGHEST_PROTOCOL)

#%%
# Create grid and buffered grid
grid = mf.create_grid_bounds(ref_segments, 1000)
grid['grid_id'] = grid.index
buffered_grid = grid.copy(deep=True)
buffered_grid.geometry = buffered_grid.geometry.buffer(10)

# TODO Speed up by doing overlay before segmentizing?

# Assign grid index to data
ref_grid = gpd.overlay(ref_segments, grid, how='intersection', keep_geom_type=False)
osm_grid = gpd.overlay(osm_segments, grid, how='intersection', keep_geom_type=False)

ref_grid_buffered = gpd.overlay(ref_segments, buffered_grid, how='intersection', keep_geom_type=False)
osm_grid_buffered = gpd.overlay(osm_segments, buffered_grid, how='intersection', keep_geom_type=False)

# Drop rows where geometries are points or multilinestring due to clipping
ref_grid = ref_grid.loc[ref_grid.geometry.geom_type == 'LineString']
osm_grid = osm_grid.loc[osm_grid.geometry.geom_type == 'LineString']
ref_grid_buffered = ref_grid_buffered.loc[ref_grid_buffered.geometry.geom_type == 'LineString']
osm_grid_buffered = osm_grid_buffered.loc[osm_grid_buffered.geometry.geom_type == 'LineString']
#%%
################################ GRID ANALYSIS ################################
ref_id_col = 'seg_id'
grid_ids = list(ref_grid.grid_id.unique())

results = [mf.analyse_grid_cell(grid_id, ref_grid, osm_grid, ref_grid_buffered, osm_grid_buffered, ref_id_col) for grid_id in grid_ids]

all_results = dict(ChainMap(*results))

osm_updated = mf.update_osm_grid(osm_data=osm_edges, ids_attr_dict=all_results, attr='vejklasse')
osm_updated.plot()

with open('../data/entire_matched_area.pickle', 'wb') as handle:
    pickle.dump(osm_updated, handle, protocol=pickle.HIGHEST_PROTOCOL)
#%%
################################ SMALL TEST ################################

# Get smaller subsets for testing
bounds = ref_segments.total_bounds

xmin = bounds[0]
xmax = xmin + 3000
ymin = bounds[1]
ymax = ymin + 3000

osm_segments = osm_segments.cx[xmin:xmax, ymin:ymax].copy(deep=True)
ref_segments = ref_segments.cx[xmin:xmax, ymin:ymax].copy(deep=True)

#%%
buffer_matches = mf.overlay_buffer(reference_data=ref_segments, osm_data=osm_segments, ref_id_col='seg_id', dist=15)

#%%

final_matches = mf.find_matches_from_buffer(buffer_matches=buffer_matches, osm_edges=osm_segments, reference_data=ref_segments, angular_threshold=30, hausdorff_threshold=17)

#%%
osm_updated = mf.update_osm(osm_segments=osm_segments, osm_data=osm_edges, final_matches=final_matches, attr='vejklasse')

osm_updated.plot()
#%%


'''
# Export matches
# Get matched and non-matched segments
matched_osm_ids = final_matches.osmid.to_list()
osm_matched = osm_segments[osm_segments['osmid'].isin(matched_osm_ids)]

matched_seg_ids = final_matches.seg_id.to_list()
ref_matched = ref_segments[ref_segments.seg_id.isin(matched_seg_ids)]

ref_not_matched = ref_segments[~ref_segments.index.isin(ref_matched.index)]

ref_not_matched.to_file('../data/ref_not_matched_april.gpkg',driver='GPKG')
ref_matched.to_file('../data/ref_matched_april.gpkg',driver='GPKG')
osm_matched.to_file('../data/osm_matched_april.gpkg', driver='GPKG')
osm_segments.to_file('../data/osm_segments_april.gpkg', driver='GPKG')
ref_segments.to_file('../data/ref_segments_april.gpkg', driver='GPKG')

osm_edges[['highway','geometry']].to_file('../data/osm_edges_april.gpkg',driver='GPKG')

osm_updated[['highway','geometry','vejklasse']].to_file('../data/osm_updated_april.gpkg',driver='GPKG')

'''

#%% 
# TODO: Quality check (optional)
# If you have data on correct matches - check result against this and compute score

if quality_check:
    # Check how many where correct compared to reference data
    # Compute percentage of correct
    # Compute percentage of not matched
    # Compute percentage of wrongly matched
    pass

#%%
#######################################################
if use_postgres:

    # Upload result to DB
    engine = dbf.connect_alc(db_name, db_user, db_password, db_port=db_port)

    dbf.to_postgis(geodataframe=osm_updated, table_name='osm_updated', engine=engine)

    dbf.to_postgis(geodataframe=final_matches, table_name='final_matches', engine=engine)

    connection.close()

else:
    with open('../data/osm_reference_match.pickle', 'wb') as handle:
        pickle.dump(final_matches, handle, protocol=pickle.HIGHEST_PROTOCOL)

    with open('../data/osm_updated.pickle', 'wb') as handle:
        pickle.dump(osm_updated, handle, protocol=pickle.HIGHEST_PROTOCOL)
#%%
#%load_ext line_profiler

#stats_lprun = %lprun -r -f mf.find_best_match mf.find_matches_from_buffer(buffer_matches=buffer_matches, osm_edges=osm_segments, reference_data=ref_segments, angular_threshold=30, hausdorff_threshold=17)

#stats_lprun.print_stats()
