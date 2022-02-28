import geopandas as gpd
import numpy as np
import pandas as pd
from scipy.spatial.distance import directed_hausdorff

def get_geom_diff(geom1, geom2):

    '''
    Function for getting the geometric difference between two geometries
    Input geometries are shapely geometries - e.g. LineStrings.
    The resulting difference is also returned as a shapely geometry.
    '''

    geoms1 = [geom1]
    geoms2 = [geom2]

    geodf1 = gpd.GeoDataFrame(geometry=geoms1)

    geodf2 = gpd.GeoDataFrame(geometry=geoms2)

    geom_diff = geodf1.difference(geodf2).values[0]

    return geom_diff


def get_angle(linestring1, linestring2):
    '''
    Function for getting the smallest angle between two lines, without considering direction of lines
    I.e. is the angle larger than 90, it is instead expressed as 180 - org angle
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



def find_matches_buffer(reference_data, osm_data, col_ref_id, dist):

    '''
    Parameters are: 
        - GeoDataFrame with edges/linestrings to be matched to OSM data
        - GeoDataFrame with OSM edges that reference data should be matched to
        - The name of the column with unique ID for reference features
        - The max distance between features that should be considered a match

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


def get_hausdorff_dist(osm_edge, ref_edge):

    '''
    Computes the Hausdorff distance (max distance) between two LineStrings
    '''

    osm_coords = list(osm_edge.coords)
    ref_coords = list(ref_edge.coords)

    hausdorff_dist = max(directed_hausdorff(osm_coords, ref_coords)[0], directed_hausdorff(ref_coords, osm_coords)[0])

    return hausdorff_dist