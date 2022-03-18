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
from timeit import timeit
import geopandas as gpd
import pandas as pd
import yaml
from src import db_functions as dbf
from src import matching_functions as mf
import osmnx as ox
import numpy as np
from scipy.spatial.distance import directed_hausdorff
from shapely.ops import nearest_points, split, linemerge, snap
from shapely.geometry import Point, MultiPoint, LineString
import momepy
import matplotlib.pyplot as plt


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

    with open('../data/osm_edges_sim.pickle', 'rb') as fp:
        osm_edges = pickle.load(fp)

    with open('../data/reference_data.pickle', 'rb') as fp:
        reference_data = pickle.load(fp)

    with open('../data/osm_nodes_sim.pickle', 'rb') as fp:
        osm_nodes = pickle.load(fp)

    
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
# Get smaller subset!

xmin = 723371
xmax = xmin + 2000
ymin = 6180833
ymax = ymin + 2000

osm_edges = osm_edges.cx[xmin:xmax, ymin:ymax].copy(deep=True)

reference_data = reference_data.cx[xmin:xmax, ymin:ymax].copy(deep=True)
reference_data = gpd.read_file('../data/ref_subset.gpkg')


id_list = [1087306852, 1210414166, 1102903365, 1102903258]

reference_data = reference_data.loc[reference_data.fot_id.isin(id_list)]
# Define name of id col in ref dataset
ref_id_col = 'fot_id'

#%%

def find_matches_buffer(reference_data, osm_data, ref_id_col, dist):

    '''
    Function for finding which OSM edges intersect a buffered reference data set.
    The first step in a matching of OSM with another line data set.

    Parameters
    ----------
    reference_data: GeoDataFrame (geopandas)
        GDF with edges (LineStrings) to be matched to OSM edges.

    osm_data: GeoDataFrame (geopandas)
        GeoDataFrame with OSM edges which the reference data should be matched to.

    ref_id_col: String
        Name of column with unique ID for reference feature

    dist: Numerical
        Max distance between distances that should be considered a match (used for creating the buffers)


    Returns
    -------
    matches DataFrame (pandas):
        DataFrame with the reference index as index, a column with reference data unique ID and the index and ID of intersecting OSM edges.

    '''

    # Functionality for accepting a series/row instead of a dataframe
    if type(reference_data) == pd.core.series.Series:
        reference_data = gpd.GeoDataFrame({ref_id_col:reference_data[ref_id_col], 'geometry':reference_data.geometry}, index=[0])

    reference_buff = reference_data.copy(deep=True)
    reference_buff.geometry = reference_buff.geometry.buffer(distance=dist)

    # Create spatial index on osm data
    osm_sindex = osm_data.sindex

    # Create dataframe to store matches in
    matches = pd.DataFrame(index=reference_buff.index, columns=['matches_index','matches_osmid', ref_id_col])

    for index, row in reference_buff.iterrows():

        # The function makes use of a spatial index to first identify potential matches before finding exact matches
   
        buffer = row['geometry']

        possible_matches_index = list(osm_sindex.intersection(buffer.bounds))

        possible_matches = osm_data.iloc[possible_matches_index]

        precise_matches = possible_matches[possible_matches.intersects(buffer)]

        precise_matches_index = list(precise_matches.index)

        precise_matches_id = list(precise_matches.osmid)

        matches.at[index, 'matches_index'] = precise_matches_index
        matches.at[index, 'matches_osmid'] = precise_matches_id

        matches.at[index, ref_id_col] = row[ref_id_col]

    return matches


# Buffer test

# Test data (both gdf and series)

# Run function

# Assert that results are as expected


def create_segments():

    # Cut dataset into small substrings
    # Create unique id
    # Maintain reference to old id

    # linear referencing
    # substring

    pass



def merge_segments():

    # Merge segments with same ID

    # Convert other attribute (i.e. which line they were matched to as list)
    # only keep unique values in list

    pass


def save_best_match(final_matches, ref_id_col, ref_id, osm_index, potential_matches, clipped_reference_geom):

    '''
    Function for saving the best of the potential matches.
    To be used internally in the function for finding the exact match between features in reference and OSM data.

    Parameters
    ----------
    osm_index: index key (string or int)
        Index of matched OSM feature

    ref_id_col: string
        Name of column with unique ID for reference data

    ref_id: string/numeric
        Unique ID of reference edge

    potential_matches: 
        Dataframe with OSM edges that are potential matches

    final_matches: pandas DataFrame
        DataFrame used to store final mathces

    clipped_reference_geometry: LineString
        Geometry for the clipped reference edge for this match


    Returns
    -------
    None:
        Updates dataframe with final matches
    '''

    osm_id = potential_matches.loc[osm_index, 'osmid']

    if final_matches.last_valid_index() == None:
        new_ix = 1
    else:
        new_ix = final_matches.last_valid_index() + 1

    final_matches.at[new_ix, ref_id_col] = ref_id
    final_matches.at[new_ix, 'osmid'] = osm_id
    final_matches.at[new_ix, 'osm_index'] = osm_index
    final_matches.at[new_ix, 'geometry'] = clipped_reference_geom



# Function for finding the best out of potential/possible matches
def find_best_match_segment(potential_matches, reference_edge, angular_threshold, hausdorff_threshold):

    '''
    Parameters
    ----------
    Returns
    -------
    '''

    # Loop through all matches and compute how good of a match they are (Hausdorff distance and angles)
    for osm_i, r in potential_matches.iterrows():

        osm_edge = r.geometry # Get the geometry of this specific matched OSM edge

        hausdorff_dist = mf.get_hausdorff_dist(osm_edge=osm_edge, ref_edge=reference_edge)
        potential_matches.at[osm_i, 'hausdorff_dist'] = hausdorff_dist

        angle_deg = mf.get_angle(osm_edge, reference_edge)
        potential_matches.at[osm_i, 'angle'] = angle_deg

        # Find matches within thresholds out of all matches for this referehce geometry
        potential_matches = potential_matches[ (potential_matches.angle < angular_threshold) & (potential_matches.hausdorff_dist < hausdorff_threshold)]

        if len(potential_matches) == 0:
            
            best_osm_ix = None

        elif len(potential_matches) == 1:
            best_osm_ix = potential_matches.index.values[0]

        else:
            # Get match(es) with smallest Hausdorff distance and angular tolerance
            potential_matches['hausdorff_dist'] = pd.to_numeric(potential_matches['hausdorff_dist'] )
            potential_matches['angle'] = pd.to_numeric(potential_matches['angle'])
            
            best_matches_index = potential_matches[['hausdorff_dist','angle']].idxmin()
            best_matches = potential_matches.loc[best_matches_index].copy(deep=True)
            
            best_matches = best_matches[~best_matches.index.duplicated(keep='first')] # Duplicates may appear if the same edge is the one with min dist and min angle

            if len(best_matches) == 1:

                best_osm_ix = best_matches.index.values[0]

            elif len(best_matches) > 1: # Take the one with the smallest hausdorff distance
                
                best_match_index = best_matches['hausdorff_dist'].idxmin()
                best_match = potential_matches.loc[best_match_index].copy(deep=True)
                best_match = best_match[~best_match.index.duplicated(keep='first')]
        
                best_osm_ix = best_match.name # Save result

    return best_osm_ix

# Function for running other matching functions
def find_matches_segments(osm_edges, reference_data, ref_id_col, buffer_dist=10, angular_threshold=30, hausdorff_threshold=12, crs='EPSG:25832'):

    final_matches = gpd.GeoDataFrame(columns = [ref_id_col,'osmid','osm_index','geometry'], crs=crs) #TODO: Consider creating multiindex from ref_ix and osm_ix?

    for _, row in reference_data.iterrows():

        ref_id = row[ref_id_col]
       
        # TODO: Check that this one works!!
        # Find matches within buffer distance
        buffer_matches = mf.find_matches_buffer(reference_data=row, osm_data=osm_edges, ref_id_col=ref_id_col, dist=buffer_dist)

         # If no matches exist at all, continue to next reference_data geometry and add the reference_data feature as unmatched
        if len(buffer_matches.loc[0,'matches_index']) < 1:

            print('No matches found with buffer!')

            continue

        else:
            ref_edge = row.geometry

            if ref_edge.geom_type == 'MultiLineString':
                # Some steps will not work for MultiLineString - convert those to LineString
                ref_edge = linemerge(ref_edge) # This step assumes that MultiLineString do not have gaps!

            # Get the original geometries that intersected this reference_data geometry's buffer
            potential_matches = osm_edges[['osmid','highway','name','geometry']].loc[buffer_matches.loc[0,'matches_index']].copy(deep=True)

            potential_matches['hausdorff_dist'] = None
            potential_matches['angle'] = None

            # TODO: Rewrite best match function for segments!
            best_osm_ix = mf.find_best_match(potential_matches=potential_matches, reference_edge=ref_edge, angular_threshold=angular_threshold, hausdorff_threshold=hausdorff_threshold)

            print('ref id', ref_id)
            print('best osm ix', best_osm_ix)

            if best_osm_ix is None:
                continue

            # Save best match
            mf.save_best_match(final_matches=final_matches, ref_id_col=ref_id_col, ref_id=ref_id, osm_index=best_osm_ix, potential_matches=osm_df, clipped_reference_geom=ref_edge)

           

    print(f'{len(final_matches)} reference edges where matched to OSM edges')

    print(f'{ len(reference_data) - len(final_matches) } reference edges where not matched')
    
    return final_matches, partially_matched, buffer_matches, potential_matches



#%%


#%%
final_matches, partially_matched, buff, osmdf = find_matches_segments(osm_edges=osm_edges, reference_data=reference_data.loc[reference_data.fot_id==1087306852], ref_id_col=ref_id_col, buffer_dist=10, angular_threshold=20, hausdorff_threshold=15, crs='EPSG:25832')

#%%
# TODO: Reassemble!

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

#%%
# TODO: Add unmatched to dataset
# Not just a question of adding to database - should create uniform col names, geometric structure (i.e. simplified or not)
# Unmatched are those not matched when rerunning partial matches

# TODO: Change to actual unmatched - this is just for testing
unmatched = None

if add_unmatched:
    
   
    # Run function for adding data

    # How will this work on a grid by grid basis? Maybe not at all?
    # Test for a city and then for increasing areas

    pass


#%%
import networkx as nx

col_dictionary = {
    'vejklasse': 'highway',
    'overflade': 'surface'
}

# niveau can indicate bridge or tunnel
# trafikart can indicate whether there are motortraffic or not

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
osm_edges[['osmid','geometry']].to_file('../data/osm_subset.gpkg', driver='GPKG')

reference_data.to_file('../data/ref_subset.gpkg', driver='GPKG')

#%%
fm = final_matches.copy(deep=True)

fm.reset_index(inplace=True, drop=True)

fm[["fot_id", "osmid",'osm_index']] = fm[["fot_id", "osmid",'osm_index']].apply(pd.to_numeric)

#pm = partially_matched.copy(deep=True)

#pm.reset_index(inplace=True, drop=True)

#pm[["fot_id", "osmid",'osm_index']] = pm[["fot_id", "osmid",'osm_index']].apply(pd.to_numeric)

# Save results
fm.to_file('../data/osm_reference_match.gpkg', layer='final_matches', driver='GPKG')
#pm.to_file('../data/osm_reference_match.gpkg', layer='partially_matched', driver='GPKG')

#%%
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


