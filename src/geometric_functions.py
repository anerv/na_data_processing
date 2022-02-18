import geopandas as gpd
import numpy as np

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