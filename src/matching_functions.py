import geopandas as gpd
import numpy as np
import pandas as pd
from scipy.spatial.distance import directed_hausdorff
from shapely.ops import nearest_points, split, linemerge, snap
from shapely.geometry import Point, MultiPoint, LineString
import numpy as np

# TODO: Add tests!

############

"""
    Counts the number of times a line occurs. Case-sensitive.

    Parameters
    ----------
    f: file
        the file to scan
    line: str
        the line to count

    Returns
    -------
    int
        the number of times the line occurs.
    """

######

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


def find_matches_buffer(reference_data, osm_data, col_ref_id, dist):

    '''
    Function for finding which OSM edges intersect a buffered reference data set.
    The first step in a matching of OSM with another line data set.

    Parameters
    ----------
    reference_data: GeoDataFrame (geopandas)
        GDF with edges (LineStrings) to be matched to OSM edges.

    osm_data: GeoDataFrame (geopandas)
        GeoDataFrame with OSM edges which the reference data should be matched to.

    col_ref_id: String
        Name of column with unique ID for reference feature

    dist: Numerical
        Max distance between distances that should be considered a match (used for creating the buffers)


    Returns
    -------
    matches DataFrame (pandas):
        DataFrame with the reference index as index, a column with reference data unique ID and the index and ID of intersecting OSM edges.


    '''

    reference_buff = reference_data.copy(deep=True)

    reference_buff.geometry = reference_buff.geometry.buffer(distance=dist)

    # Create spatial index on osm data
    osm_sindex = osm_data.sindex

    # Create dataframe to store matches in
    matches = pd.DataFrame(index=reference_buff.index, columns=['matches_index','matches_osmid', col_ref_id])

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

        matches.at[index, col_ref_id] = row[col_ref_id]

    return matches


def save_best_match(osm_index, ref_index, ref_id_col, row, potential_matches, final_matches, partially_matched, clipped_edges, pct_removed_threshold, meters_removed_threshold):

    '''
    Function for saving the best of the potential matches.
    To be used internally in the function for finding the exact match between features in reference and OSM data.
    Also check how much of the final match is clipped/removed for the match, and checks for full/partial match.

    Parameters
    ----------
    osm_index: index key (string or int)
        Index of matched OSM feature

    ref_index: index key (string or int)
        Index of matched reference feature

    ref_id_col: string
        Name of column with unique ID for reference data

    row: pandas row
        Row in matched reference data currently being looked at

    potential_matches: 
        Dataframe with OSM edges that are potential matches

    final_matches: pandas DataFrame
        DataFrame used to store final mathces

    partially_matched: pandas DataFrame
        DataFrame used to store partial matches

    clipped_edges: pandas DataFrame
        DataFrame storing how much and to what reference edges have been clipped

    pct_removed_threshold: float/int
        Threshold for how many percent of an edge can be clipped before it is considered only partially matched

    meters_removed_threshold: float/int
        Threshold for how many meters of an edge can be clipped before it is considered only partially matched

    Returns
    -------
    None:
        Updates dataframe with final matches
    '''

    final_matches.at[ref_index, ref_id_col] = row[ref_id_col]
    final_matches.at[ref_index, 'osmid'] = potential_matches.loc[osm_index, 'osmid']
    final_matches.at[ref_index, 'osm_index'] = osm_index
    final_matches.at[ref_index, 'geometry'] = potential_matches.loc[osm_index, 'geometry']

    if clipped_edges.loc[osm_index, 'pct_removed'] > pct_removed_threshold and clipped_edges.loc[osm_index, 'meters_removed'] > meters_removed_threshold:
    
        #Save removed geometries
        removed_edge = clipped_edges.loc[osm_index, ['geometry']].values[0]
        partially_matched.at[ref_index, 'geometry'] = removed_edge
        partially_matched.at[ref_index, ref_id_col] = row[ref_id_col]
        partially_matched.at[ref_index, 'meters_removed'] = clipped_edges.loc[osm_index, 'pct_removed']
        partially_matched.at[ref_index, 'pct_removed'] = clipped_edges.loc[osm_index, 'meters_removed']


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

    angular_threshold: float/int
        Threshold for angular difference between features than can be considered matches

    hausdorff_threshold: float/int
        Threshold for Hausdorff distance between features than can be considered matches

    pct_removed_threshold: float/int
        Threshold for how many pct of the length can be cut from reference edge before it is only considered partially matched.

    meters_removed_threshold: float/int
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

    angular_threshold = 30 # threshold in degress
    hausdorff_threshold = 15 # threshold in meters

    pct_removed_threshold = 20 # in pct
    meters_removed_threshold = 5 # in meters

    
    for ref_index, row in matches.iterrows(): # TODO: Use something else than iterrows for better performance?

        # If no matches exist at all, continue to next reference_data geometry and add the reference_data feature as unmatched
        if len(row.matches_index) < 1:

            continue

        else:

            # Get the original geometry for the reference_data feature
            ref_edge = reference_data.loc[ref_index].geometry

            if ref_edge.geom_type == 'MultiLineString': 
                # Some steps will not work for MultiLineString - convert those to LineString
                ref_edge = linemerge(ref_edge) # This step assumes that MultiLineString do not have gaps!

            # Get the original geometries that intersected this reference_data geometry's buffer
            osm_df = osm_edges[['osmid','highway','name','geometry']].iloc[row.matches_index].copy(deep=True)

            osm_df['hausdorff_dist'] = None
            osm_df['angle'] = None
            
            #Dataframe for storing how much is removed from the reference_data geometry with the different OSM matches
            clipped_edges = gpd.GeoDataFrame(index=osm_df.index, columns=['clipped_length','meters_removed','pct_removed',ref_id_col,'geometry'], crs=crs)
            clipped_edges['geometry'] = None

            # Loop through all matches and compute how good of a match they are (Hausdorff distance and angles)
            for osm_i, r in osm_df.iterrows():

                osm_edge = r.geometry # Get the geometry of this specific matched OSM edge

                clipped_ref_edge = clip_new_edge(line_to_split=ref_edge, split_line=osm_edge)
            
                meters_removed = ref_edge.length - clipped_ref_edge.length
                pct_removed = meters_removed * 100 / ref_edge.length

                clipped_edges.at[osm_i,'clipped_length'] = clipped_ref_edge.length
                clipped_edges.at[osm_i,'meters_removed'] = meters_removed
                clipped_edges.at[osm_i,'pct_removed'] = pct_removed
                clipped_edges.at[osm_i, ref_id_col] = row[ref_id_col]
                clipped_edges.at[osm_i, 'geometry'] = get_geom_diff(ref_edge, clipped_ref_edge.buffer(0.001))

                # If length of clipped reference_data edge is very small it indicates that the OSM edge is perpendicular with the reference_data edge and thus not a correct match
                if clipped_ref_edge.length < 1:

                    clipped_edges.drop(labels=osm_i, inplace=True)
                    
                    continue

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

                save_best_match(osm_index=osm_ix, ref_index=ref_index, ref_id_col=ref_id_col, row=row, potential_matches=osm_df,final_matches=final_matches, partially_matched=ref_part_matched, clipped_edges=clipped_edges, pct_removed_threshold=pct_removed_threshold, meters_removed_threshold=meters_removed_threshold)
            
            else:

                # Get match(es) with smallest Hausdorff distance and angular tolerance
                osm_df['hausdorff_dist'] = pd.to_numeric(osm_df['hausdorff_dist'] )
                osm_df['angle'] = pd.to_numeric(osm_df['angle'])
                
                best_matches_index = osm_df[['hausdorff_dist','angle']].idxmin()
                best_matches = osm_df.loc[best_matches_index].copy(deep=True)
                
                best_matches = best_matches[~best_matches.index.duplicated(keep='first')] # Duplicates may appear if the same edge is the one with min dist and min angle

                if len(best_matches) == 1:

                    osm_ix = best_matches.index.values[0]

                    save_best_match(osm_index=osm_ix, ref_index=ref_index, ref_id_col=ref_id_col, row=row, potential_matches=osm_df,final_matches=final_matches, partially_matched=ref_part_matched, clipped_edges=clipped_edges, pct_removed_threshold=pct_removed_threshold, meters_removed_threshold=meters_removed_threshold)

                elif len(best_matches) > 1: # Take the one with the smallest hausdorff distance
                    
                    best_match_index = best_matches['hausdorff_dist'].idxmin()
                    best_match = osm_df.loc[best_match_index].copy(deep=True)
                    best_match = best_match[~best_match.index.duplicated(keep='first')]
            
                    osm_ix = best_match.name # Save result

                    save_best_match(osm_index=osm_ix, ref_index=ref_index, ref_id_col=ref_id_col, row=row, potential_matches=osm_df,final_matches=final_matches, partially_matched=ref_part_matched, clipped_edges=clipped_edges, pct_removed_threshold=pct_removed_threshold, meters_removed_threshold=meters_removed_threshold)
    
    final_matches.dropna(inplace=True)
    ref_part_matched.dropna(inplace=True)

    print(f'{len(final_matches)} reference edges where matched to OSM edges')
    print(f'Out of those, {len(ref_part_matched)} reference edges where only partially matched to OSM edges')
    print(f'{ len(reference_data) - len(final_matches) } reference edges where not matched')
    
    return final_matches, ref_part_matched


def update_osm(matches, osm_data, ref_data, ref_col, new_col, compare_col=None):

    '''
    Function for updating OSM based on reference. 
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

    count_updates = len(osm_edges.loc[final_matches.osm_index])

    print(f'{count_updates} OSM edges were updated!')

    if compare_col:
        diff = count_updates - np.count_nonzero(osm_edges[compare_col])
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


if __name__ == '__main__':
    pass


# Test get_geom_diff

# Test get_angle

# Test get_hausdorff_dist

# Test clip_new_edge

# Get test data and check higher order functions (this data can also be used for quality check?)