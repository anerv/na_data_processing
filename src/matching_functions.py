import geopandas as gpd
import numpy as np
import pandas as pd
from scipy.spatial.distance import directed_hausdorff
from shapely.ops import nearest_points, split, linemerge, snap
from shapely.geometry import Point, MultiPoint, LineString
import momepy
import osmnx as ox


# TODO: Add tests!


##############################

def get_geom_diff(geom1, geom2):

    '''
    Function for getting the geometric difference between two geometries.
    Input geometries are Shapely geometries - e.g. LineStrings.
    The resulting difference is also returned as a Shapely geometry.
    Makes use of Geopandas difference function.
    The order in which the geometries are passed to the function does not matter.

    Parameters
    ----------
    geom1: Shapely geometry
        The first geometry


    geom2: Shapely geometry
        The second geometry

    Returns
    -------
    geom_diff
        The geomtric difference in the form of a Shapely geometry.
    '''

    geoms1 = [geom1]
    geoms2 = [geom2]

    geodf1 = gpd.GeoDataFrame(geometry=geoms1)

    geodf2 = gpd.GeoDataFrame(geometry=geoms2)

    geom_diff = geodf1.difference(geodf2).values[0]

    return geom_diff

##############################

def get_angle(linestring1, linestring2):

    '''
    Function for getting the smallest angle between two lines.
    Does not consider the direction of lines: I.e. is the angle larger than 90, it is instead expressed as 180 - original angle.

    Parameters
    ----------
    linestring1: Shapely geometry

    linestring2: Shapely geometry

    Returns
    -------
    angle_deg: float
        angle expressed in degrees

    '''

    arr1 = np.array(linestring1.coords)
    arr1 = arr1[1] - arr1[0]

    arr2 = np.array(linestring2.coords)
    arr2 = arr2[1] - arr2[0]

    angle = np.math.atan2(np.linalg.det([arr1,arr2]),np.dot(arr1,arr2))
    angle_deg = abs(np.degrees(angle))

    if angle_deg > 90:
        angle_deg = 180 - angle_deg

    return angle_deg

##############################

def get_hausdorff_dist(osm_edge, ref_edge):

    '''
    Computes the Hausdorff distance (max distance) between two LineStrings.

    Parameters
    ----------

    osm_edge: Shapely LineString
        The first geometry to compute Hausdorff distance between.

    ref_edge: Shapely LineString
        Second geometry to be used in distance calculation.

    Returns
    -------
    hausdorff_dist: float
        The Hausdorff distance.

    '''

    osm_coords = list(osm_edge.coords)
    ref_coords = list(ref_edge.coords)

    hausdorff_dist = max(directed_hausdorff(osm_coords, ref_coords)[0], directed_hausdorff(ref_coords, osm_coords)[0])

    return hausdorff_dist

##############################

def clip_new_edge(line_to_split, split_line):

    '''
    Clips one LineString to the extent of another LineString

    Due to limitations in coordinate precision in Shapely, the function uses a workaround where a new LineString is constructed based on clip points snapped to the original line.
    (Rather than using the Shapely clip function, which in case of floating point imprecision will fail for many instances).

    Parameters
    ----------
    line_to_split: Shapely LineString

    split_line: Shapely LineString


    Returns
    -------
    clipped_line: Shapely Linestring
        First line clipped to the extent of the second line.

    '''

    start_node = Point(split_line.coords[0])
    end_node = Point(split_line.coords[-1])

    # Get nearest point on reference geometry to start and end nodes of OSM match
    _, nearest_point_start = nearest_points(start_node, line_to_split)
    _, nearest_point_end = nearest_points(end_node, line_to_split)
        
    new_nearest_start = snap(nearest_point_start, line_to_split, 0.01)
    new_nearest_end = snap(nearest_point_end, line_to_split, 0.01)

    clipped_line = LineString( [new_nearest_start, new_nearest_end] )

    return clipped_line

##############################

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


    # Functionality for accepting a series instead of a dataframe
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


##############################

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

   
##############################


def partial_match(clipped_ref_geom, org_ref_geom, meters_removed_threshold, ref_id_col, ref_id):

    diff = org_ref_geom.length - clipped_ref_geom.length

    if diff < meters_removed_threshold:

        ref_geom = org_ref_geom

        if ref_geom.geom_type == 'MultiLineString': 
            # Some steps will not work for MultiLineString - convert those to LineString
            ref_geom = linemerge(ref_geom) # This step assumes that MultiLineString do not have gaps!

        partially_matched_split = None

    else:

        ref_geom = clipped_ref_geom
        clipped_parts = get_geom_diff(org_ref_geom, ref_geom)

        partial_df = gpd.GeoDataFrame(geometry=[clipped_parts])

        partial_df[ref_id_col] = ref_id

        # Split multilinestrings in partially matched into individual LineStrings
        #partially_matched_split = explode_multilinestrings(partial_df)
        partially_matched_split = partial_df.explode(ignore_index=True)

        partially_matched_split[partially_matched_split.geometry.length > 2]


    return ref_geom, partially_matched_split

##############################

def find_best_match(potential_matches, reference_edge, angular_threshold, hausdorff_threshold):

    '''
    Parameters
    ----------
    Returns
    -------
    '''

    # Loop through all matches and compute how good of a match they are (Hausdorff distance and angles)
    for osm_i, r in potential_matches.iterrows():

        osm_edge = r.geometry # Get the geometry of this specific matched OSM edge

        #TODO: Find a way of solving problem when OSM is much longer than reference data - problems with Hausdorff distance!
        clipped_ref_edge = clip_new_edge(line_to_split=reference_edge, split_line=osm_edge)

        if clipped_ref_edge.length < 3:

            best_osm_ix = None
            continue

        hausdorff_dist = get_hausdorff_dist(osm_edge=osm_edge, ref_edge=clipped_ref_edge)
        potential_matches.at[osm_i, 'hausdorff_dist'] = hausdorff_dist

        angle_deg = get_angle(osm_edge, reference_edge)
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

    return clipped_ref_edge, best_osm_ix


#############################
'''
def final_partial_match(final_matches, reference_data, ref_id_col, meters_removed_threshold):

    ref_part_matched = pd.DataFrame(index = reference_data.index, columns=[ref_id_col, 'meters_removed'])

    # At the end compare summed length of rows belonging to each ref id with org length? If above some threshold, mark it as only partially matched

    # Create Dataframe with summed lengths and ref id col
    # Group final matches by ref id col
    # Get matched length

    # Get meter difference and pct difference between sum and org

    # If above threshold:
        # Save to ref_part matched

    ref_part_matched.dropna(inplace=True)

    return ref_part_matched

'''



#############################


def find_matches(osm_edges, reference_data, ref_id_col, buffer_dist=12, angular_threshold=30, hausdorff_threshold=15, meters_removed_threshold=6, crs='EPSG:25832'):

    final_matches = gpd.GeoDataFrame(columns = [ref_id_col,'osmid','osm_index','geometry'], crs=crs) #TODO: Consider creating multiindex from ref_ix and osm_ix?

    for _, row in reference_data.iterrows():

        ref_id = row[ref_id_col]
        print(ref_id)

        buffer_matches = find_matches_buffer(reference_data=row, osm_data=osm_edges, ref_id_col=ref_id_col, dist=buffer_dist)

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
            osm_df = osm_edges[['osmid','highway','name','geometry']].loc[buffer_matches.loc[0,'matches_index']].copy(deep=True)

            osm_df['hausdorff_dist'] = None
            osm_df['angle'] = None

            clipped_ref_edge, best_osm_ix = find_best_match(potential_matches=osm_df, reference_edge=ref_edge, angular_threshold=angular_threshold, hausdorff_threshold=hausdorff_threshold)

            print('ref id', ref_id)
            print('best osm ix', best_osm_ix)

            if best_osm_ix is None:
                continue

            # Chech for partial match
            ref_edge, partial_df = mf.partial_match(clipped_ref_geom=clipped_ref_edge, org_ref_geom=ref_edge, meters_removed_threshold=meters_removed_threshold, ref_id_col=ref_id_col, ref_id=ref_id)

            # Save best match
            save_best_match(final_matches=final_matches, ref_id_col=ref_id_col, ref_id=ref_id, osm_index=best_osm_ix, potential_matches=osm_df, clipped_reference_geom=ref_edge)

            if partial_df is not None: # Add remaining parts to edges to be matched.
                print('Partial match found!')

                for _, row in partial_df.iterrows():
                    new_ix = reference_data.last_valid_index() + 1
                    reference_data.at[new_ix, ref_id_col] = row[ref_id_col]
                    reference_data.at[new_ix, 'geometry'] = row['geometry']


    
    #final_matches.dropna(inplace=True)

    partially_matched = None
    #partially_matched = final_partial_match(reference_data, final_matches)

    print(f'{len(final_matches)} reference edges where matched to OSM edges')
    #print(f'Out of those, {len(partially_matched)} reference edges where only partially matched to OSM edges')
    print(f'{ len(reference_data) - len(final_matches) } reference edges where not matched')
    
    return final_matches, partially_matched, buffer_matches, osm_df



#############################



def update_osm(matches, osm_data, ref_data, ref_col, new_col, compare_col= None):

    '''
    Function for updating OSM based on matches with reference dataset. 
    Current version only accepts one column for reference data and one column in OSM data.

    Parameters
    ----------
    matched_data: GeoDataFrame
        Index and ids of matches

    osm_data: GeoDataFrame
        OSM edges to be updated

    ref_data: GeoDataFrame
        Reference data with attributes to be transfered to OSM data

    Returns
    -------
    
    '''

    osm_data[new_col] = None

    ref_matches = ref_data[ref_col].loc[matches.index].values

    osm_data.loc[matches.osm_index, new_col] = ref_matches

    count_updates = len(osm_data.loc[matches.osm_index])

    print(f'{count_updates} OSM edges were updated!')

    if compare_col:
        diff = count_updates - np.count_nonzero(osm_data[compare_col])
        print(f'{diff} OSM edges did not already have this information!')

    # Check for conflicting updates
    count_conflicts = 0

    duplicates = matches.osm_index.value_counts()
    duplicates = duplicates[duplicates > 1]
    dup_index = duplicates.index.to_list()

    for i in dup_index:

        same_matches = matches.loc[matches.osm_index == i].index
        ref_values = ref_data.loc[same_matches, ref_col]
        
        if len(ref_values.unique()) > 1:
         
            count_conflicts += 1
        
    print(f'{count_conflicts} OSM edges had conflicting matches!')

    return osm_data

##############################

def create_node_index(x, index_length):
    '''
    Function for creating unique index column of specific length based on another shorter column.
    '''

    x = str(x)
    x  = x.zfill(index_length)
    x = x + 'R'

    return x

##############################

def find_parallel_edges(edges):

    '''
    Check for parallel edges in a pandas DataFrame with edges, including columns u with start node index and v with end node index.
    If two edges have the same u-v pair, the column 'key' is updated to ensure that the u-v-key combination can uniquely identify an edge.
    '''

    # Find edges with duplicate node pairs
    parallel = edges[edges.duplicated(subset=['u','v'])]

    edges.loc[parallel.index, 'key'] = 1 #Set keys to 1

    assert len(edges[edges.duplicated(subset=['u','v','key'])]) == 0, 'Edges not uniquely indexed by u,v,key!'

    return edges

##############################

def create_osmnx_graph(gdf, directed=True):

    ''''
    Function for  converting a geodataframe with LineStrings to a NetworkX graph object (MultiDiGraph), which follows the data structure required by OSMnx.
    (I.e. Nodes indexed by osmid, nodes contain columns with x and y coordinates, edges is multiindexed by u, v, key).
    Converts MultiLineStrings to LineStrings - assumes that there are no gaps between the lines in the MultiLineString

    OBS! Current version does not fix topology.

    Parameters
    ----------
    gdf: GeoDataFrame
        The data to be converted to a graph format
    directed: bool
        Whether the resulting graph should be directed or not. Directionality is based on the order of the coordinates.

    Returns
    -------
    graph: NetworkX MultiDiGraph object
        The original data in a NetworkX graph format.

    '''

    gdf['geometry'] = gdf['geometry'].apply( lambda x: linemerge(x) if x.geom_type == 'MultiLineString' else x) 

    G = momepy.gdf_to_nx(gdf, approach='primal', directed=directed)

    nodes, edges = momepy.nx_to_gdf(G)

    # Create columns and index as required by OSMnx
    index_length = len(str(nodes['nodeID'].iloc[-1].item()))
    nodes['osmid'] = nodes['nodeID'].apply(lambda x: create_node_index(x, index_length))

    # Create x y coordinate columns
    nodes['x'] = nodes.geometry.x
    nodes['y'] = nodes.geometry.y

    edges['u'] = nodes['osmid'].loc[edges.node_start].values
    edges['v'] = nodes['osmid'].loc[edges.node_end].values

    nodes.set_index('osmid', inplace=True)

    edges['key'] = 0

    edges = find_parallel_edges(edges)

    # Create multiindex in u v key format
    edges = edges.set_index(['u', 'v', 'key'])

    # For ox simplification to work, edge geometries must be dropped. Edge geometries is defined by their start and end node
    edges.drop(['geometry'], axis=1, inplace=True)


    G_ox = ox.graph_from_gdfs(nodes, edges)

   
    return G_ox

##############################

def explode_multilinestrings(gdf):

    individual_linestrings = gdf.explode(index_parts=True)

    new_ix_col = ['_'.join(map(str, i)) for i in zip(individual_linestrings.index.get_level_values(0), individual_linestrings.index.get_level_values(1))]
    individual_linestrings['index_split'] =  new_ix_col
    individual_linestrings.set_index('index_split', inplace=True)

    return individual_linestrings

##############################

def clean_col_names(df):
    '''
    Remove upper-case letters and : from OSM key names
    '''
    df.columns = df.columns.str.lower()

    df_cols = df.columns.to_list()

    new_cols = [c.replace(':','_') for c in df_cols]

    df.columns = new_cols

    return df
    

if __name__ == '__main__':
    pass


# Test get_geom_diff

# Test get_angle

# Test get_hausdorff_dist

# Test clip_new_edge

# Get test data and check higher order functions (this data can also be used for quality check?)


    def find_exact_matches(matches, osm_edges, reference_data, ref_id_col, angular_threshold=20, hausdorff_threshold=15, pct_removed_threshold=20, meters_removed_threshold=5, crs='EPSG:25832'):

        '''
        Parameters
        ----------
        matches: pandas DataFrame
            Dataframe with potential OSM matches for each reference features (based on intersection of buffered reference geometries)

        osm_edges: geopandas DataFrame
            OSM edges to be matched

        reference_data: geopandas DataFrame
            reference edges to be matched to OSM

        ref_id_col: String
            Name of column with unique ID for reference data

        angular_threshold: float/int (degrees)
            Threshold for angular difference between features than can be considered matches 

        hausdorff_threshold: float/int (meters)
            Threshold for Hausdorff distance between features than can be considered matches

        pct_removed_threshold: float/int (pct)
            Threshold for how many pct of the length can be cut from reference edge before it is only considered partially matched.

        meters_removed_threshold: float/int (meters)
            Threshold for how many meters can be cut from reference edge before it is only considered partially matched.

        Returns
        -------
        final_matches: pandas DataFrame
            DataFrame with the final matches of reference and OSM data

        ref_part_matches: pandas DataFrame
            Dataframe with reference edges that have only been partially matched
        '''

        ref_part_matched = gpd.GeoDataFrame(index = reference_data.index, columns=[ref_id_col, 'meters_removed','pct_removed','geometry'], crs=crs)
        final_matches = gpd.GeoDataFrame(index = reference_data.index, columns = [ref_id_col,'osmid','osm_index','geometry'], crs=crs)

        #angular_threshold = 30 # threshold in degress
        #hausdorff_threshold = 15 # threshold in meters

        #pct_removed_threshold = 20 # in pct
        #meters_removed_threshold = 5 # in meters

        
        for ref_index, row in matches.iterrows(): # TODO: Use something else than iterrows for better performance?

            # If no matches exist at all, continue to next reference_data geometry and add the reference_data feature as unmatched
            if len(row.matches_index) < 1:

                continue

            else:

                # While something...

                # Get the original geometry for the reference_data feature
                ref_edge = reference_data.loc[ref_index].geometry

                if ref_edge.geom_type == 'MultiLineString': 
                    # Some steps will not work for MultiLineString - convert those to LineString
                    ref_edge = linemerge(ref_edge) # This step assumes that MultiLineString do not have gaps!

                # Get the original geometries that intersected this reference_data geometry's buffer
                osm_df = osm_edges[['osmid','highway','name','geometry']].loc[row.matches_index].copy(deep=True)

                osm_df['hausdorff_dist'] = None
                osm_df['angle'] = None
                

                # Loop through all matches and compute how good of a match they are (Hausdorff distance and angles)
                for osm_i, r in osm_df.iterrows():

                    osm_edge = r.geometry # Get the geometry of this specific matched OSM edge

                    #TODO: Find a way of solving problem when OSM is much longer than reference data - problems with Hausdorff distance!
                    clipped_ref_edge = clip_new_edge(line_to_split=ref_edge, split_line=osm_edge)

                    hausdorff_dist = get_hausdorff_dist(osm_edge=osm_edge, ref_edge=clipped_ref_edge)
                    osm_df.at[osm_i, 'hausdorff_dist'] = hausdorff_dist

                    angle_deg = get_angle(osm_edge, ref_edge)
                    osm_df.at[osm_i, 'angle'] = angle_deg

                # Find matches within thresholds out of all matches for this referehce geometry
                potential_matches = osm_df[ (osm_df.angle < angular_threshold) & (osm_df.hausdorff_dist < hausdorff_threshold)]

                if len(potential_matches) == 0:

                    continue
                
                elif len(potential_matches) == 1:

                    osm_ix = potential_matches.index.values[0]

                    save_best_match(osm_index=osm_ix, ref_index=ref_index, ref_id_col=ref_id_col, row=row, potential_matches=osm_df,final_matches=final_matches, clipped_ref_geom=clipped_ref_edge)
                
                    # TODO: Check if its only a partial match! If so recompute!

                else:

                    # Get match(es) with smallest Hausdorff distance and angular tolerance
                    osm_df['hausdorff_dist'] = pd.to_numeric(osm_df['hausdorff_dist'] )
                    osm_df['angle'] = pd.to_numeric(osm_df['angle'])
                    
                    best_matches_index = osm_df[['hausdorff_dist','angle']].idxmin()
                    best_matches = osm_df.loc[best_matches_index].copy(deep=True)
                    
                    best_matches = best_matches[~best_matches.index.duplicated(keep='first')] # Duplicates may appear if the same edge is the one with min dist and min angle

                    if len(best_matches) == 1:

                        osm_ix = best_matches.index.values[0]

                        save_best_match(osm_index=osm_ix, ref_index=ref_index, ref_id_col=ref_id_col, row=row, potential_matches=osm_df,final_matches=final_matches, clipped_ref_geom=clipped_ref_edge)
                        
                        # TODO: Check if its only a partial match! If so recompute!


                    elif len(best_matches) > 1: # Take the one with the smallest hausdorff distance
                        
                        best_match_index = best_matches['hausdorff_dist'].idxmin()
                        best_match = osm_df.loc[best_match_index].copy(deep=True)
                        best_match = best_match[~best_match.index.duplicated(keep='first')]
                
                        osm_ix = best_match.name # Save result

                        save_best_match(osm_index=osm_ix, ref_index=ref_index, ref_id_col=ref_id_col, row=row, potential_matches=osm_df,final_matches=final_matches, clipped_ref_geom=clipped_ref_edge)
        
                        # TODO: Check if its only a partial match! If so recompute!

        final_matches.dropna(inplace=True)
        ref_part_matched.dropna(inplace=True)

        print(f'{len(final_matches)} reference edges where matched to OSM edges')
        print(f'Out of those, {len(ref_part_matched)} reference edges where only partially matched to OSM edges')
        print(f'{ len(reference_data) - len(final_matches) } reference edges where not matched')
        
        return final_matches, ref_part_matched, osm_df, potential_matches,
