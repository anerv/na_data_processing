'''
Script for matching road networks.
Method assumes a simplified version of public dataset and a non-simplified version of OSM (osm_i.e. that edges in the reference_data dataset usually are as long or longer than OSM).
Use case is situations where a specific attribute is poorly defined in OSM, and the script is thus developed for a situation where it is assumed that most core road geometries are present in both data sets.
But it is also possible to use for identifying edges in the reference_data dataset with no match in OSM or vice-versa.

'''
# TODO: Docs

# TODO: Functionality for doing analysis grid by grid


#%%
import pickle
import geopandas as gpd
import pandas as pd
import yaml
from src import db_functions as dbf
from src import matching_functions as mf
import osmnx as ox
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

    add_unmatched = parsed_yaml_file['add_unmatched']
  
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
    #with open('../data/osm_reference_match.pickle', 'rb') as fp:
        #final_matches2 = pickle.load(fp)

    with open('../data/osm_edges.pickle', 'rb') as fp:
        osm_edges = pickle.load(fp)

    with open('../data/reference_data.pickle', 'rb') as fp:
        reference_data = pickle.load(fp)

    with open('../data/osm_nodes.pickle', 'rb') as fp:
        reference_data = pickle.load(fp)

    
    highway_values = ['residential', 'service', 'primary', 'tertiary',
            'tertiary_link', 'secondary', 'cycleway', 'path', 'living_street',
            'unclassified', 'primary_link', 'motorway_link', 'motorway', 'track',
            'secondary_link', 'pathway', 'trunk_link', 'trunk'
            ]

    osm_edges = osm_edges.loc[osm_edges['highway'].isin(highway_values)]

    reference_data = reference_data.loc[reference_data['vejklasse'].isin(['Cykelbane langs vej', 'Cykelsti langs vej'])]


assert osm_edges.crs == crs
assert reference_data.crs == crs
#assert grid.crs == crs

print(f'Number of rows in osm_edge table: {len(osm_edges)}')
print(f'Number of rows in reference_data table: {len(reference_data)}')
#print(f'Number of rows in grid table: {len(grid)}')

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

# Split multilinestrings in partially matched into individual LineStrings
partially_matched_split = mf.explode_multilinestrings(partially_matched)

# For how long? Look at results from each run - are they valid?

#%%

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

#%%
# TODO: Add unmatched to dataset
# Not just a question of adding to database - should create uniform col names, geometric structure (i.e. simplified or not)
# Unmatched are those not matched when rerunning partial matches

# TODO: Change to actual unmatched - this is just for testing
unmatched = partially_matched_split.copy(deep=True)

if add_unmatched:
    
   
    # Run function for adding data

    # How will this work on a grid by grid basis? Maybe not at all?
    # Test for a city and then for increasing areas

    pass


#%%
import networkx as nx

col_dictionary = {

}

def add_unmatched_data(osm_edges, osm_nodes, unmatched_edges):

    # Should check whether the unmatched network is of the same graph type as OSM network

    # Assert that they are the same reference system

    if osm_edges.crs != unmatched_edges.crs:

        unmatched_edges = unmatched_edges.to_crs(osm_edges.crs)

        assert osm_edges.crs == unmatched_edges.crs
    
    # Run function for converting to osmnx format
    unmatched_graph = mf.create_osmnx_graph(unmatched_edges)

    # Create new graph object from OSM edges

    # Read nodes

    # Recreate multiindex

    osm_graph = ox.graph_from_gdfs(osm_nodes, osm_edges)

    assert unmatched_graph.crs == osm_graph.crs, 'CRS do not match!'


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
# Save results
final_matches.to_file('../data/osm_reference_match.gpkg', layer='final_matches', driver='GPKG')
partially_matched.to_file('../data/osm_reference_match.gpkg', layer='partially_matched', driver='GPKG')
updated_osm.to_file('../data/osm_reference_match.gpkg', layer='updated_osm', driver='GPKG')
#%%
with open('../data/osm_reference_match.pickle', 'wb') as handle:
    pickle.dump(final_matches, handle, protocol=pickle.HIGHEST_PROTOCOL)

with open('../data/buffer_matches.pickle', 'wb') as handle:
    pickle.dump(matches, handle, protocol=pickle.HIGHEST_PROTOCOL)

with open('../data/partial_matches.pickle', 'wb') as handle:
    pickle.dump(partially_matched, handle, protocol=pickle.HIGHEST_PROTOCOL)

with open('../data/updated_osm.pickle', 'wb') as handle:
    pickle.dump(updated_osm, handle, protocol=pickle.HIGHEST_PROTOCOL)
#%%
#############################################


# %%
