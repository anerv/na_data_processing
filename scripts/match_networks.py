'''
Script for matching road networks.
Method assumes a simplified version of public dataset and a non-simplified version of OSM (osm_i.e. that edges in the reference_data dataset usually are as long or longer than OSM).
Use case is situations where a specific attribute is poorly defined in OSM, and the script is thus developed for a situation where it is assumed that most core road geometries are present in both data sets.
But it is also possible to use for identifying edges in the reference_data dataset with no match in OSM or vice-versa.

'''
# TODO: Docs

# TODO: Functionality for doing analysis grid by grid

# TODO: Rewrite to not use postgres?

# Problem with curved lines
# Problem when OSM is much longer? - Use simplified networks!

#%%
from configparser import DuplicateOptionError
import geopandas as gpd
import pandas as pd
import yaml
from src import db_functions as dbf
from src import matching_functions as mf
#%%

with open(r'config.yml') as file:
    parsed_yaml_file = yaml.load(file, Loader=yaml.FullLoader)

    crs = parsed_yaml_file['CRS']

    db_name = parsed_yaml_file['db_name']
    db_user = parsed_yaml_file['db_user']
    db_password = parsed_yaml_file['db_password']
    db_host = parsed_yaml_file['db_host']
    db_port = parsed_yaml_file['db_port']

    quality_check = parsed_yaml_file['quality_check']
    quality_data = parsed_yaml_file['quality_data']

    add_unmatched = parsed_yaml_file['add_unmatched']
  
print('Settings loaded!')

#%%
connection = dbf.connect_pg(db_name, db_user, db_password)

get_osm = '''SELECT * FROM osm_edges WHERE highway IN ('residential', 'service', 'primary', 'tertiary',
       'tertiary_link', 'secondary', 'cycleway', 'path', 'living_street',
       'unclassified', 'primary_link', 'motorway_link', 'motorway', 'track',
       'secondary_link', 'pathway', 'trunk_link', 'trunk');'''

get_geodk = 'SELECT * FROM geodk_bike;'

get_grid = 'SELECT * FROM grid;'

osm_edges = gpd.GeoDataFrame.from_postgis(get_osm, connection, geom_col='geometry' )

reference_data = gpd.GeoDataFrame.from_postgis(get_geodk, connection, geom_col='geometry' )

grid = gpd.GeoDataFrame.from_postgis(get_grid, connection, geom_col='geometry')

assert osm_edges.crs == crs
assert reference_data.crs == crs
assert grid.crs == crs

print(f'Number of rows in osm_edge table: {len(osm_edges)}')
print(f'Number of rows in reference_data table: {len(reference_data)}')
print(f'Number of rows in grid table: {len(grid)}')

#%%
# Define name of id col in ref dataset
ref_id_col = 'fot_id'

#%%
# Find matches based on buffer distance
matches = mf.find_matches_buffer(reference_data=reference_data, osm_data=osm_edges, col_ref_id =ref_id_col, dist=12) 

#%%
# Find exact matches 
final_matches, partially_matched = mf.find_exact_matches(matches=matches, osm_edges=osm_edges, reference_data=reference_data, 
ref_id_col=ref_id_col, crs=crs)

#%%

# TODO: Rerun for partial matches

# First, remove already matched OSM edges from analysis? Or maybe not - there can be more than one match in case of bike lanes on both sides of a street?

# Repeat buffer operation for this step

# Split multilinestrings in partially matched into two rows 

# For how long? Look at results from each run - are they valid?


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
# Update OSM based on matches

ref_col = 'vejklasse'
new_col = 'cycling_infra'

updated_osm = mf.update_osm(final_matches, osm_edges, reference_data, ref_col=ref_col, new_col=new_col)

#%%#%%

#%%
# TODO: Add unmatched to dataset
# Not just a question of adding to database - should create uniform col names, geometric structure (i.e. simplified or not)

if add_unmatched:
    
    # Run function for converting to osmnx format

    # Run function for adding data

    # How will this work on a grid by grid basis? Maybe not at all?
    # Test for a city and then for increasing areas

    pass

def add_unmatched_data(osm_graph, unmatched_graph, column_dictionary):

    # Should check whether the unmatched network is of the same graph type as OSM network

    # Assert that they are the same reference system

    combined_graph = None

    return combined_graph

    
#%%
#######################################################

# Upload result to DB
engine = dbf.connect_alc(db_name, db_user, db_password, db_port=db_port)

not_matched = None

dbf.to_postgis(geodataframe=not_matched, table_name='not_matched', engine=engine)

connection.close()

#%%
#############################################
# %%
# Compute how much have been updated