import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.ops import linemerge, substring
from shapely.geometry import MultiLineString
import math

def _get_segments(linestring, seg_length):

    '''
    Convert a Shapely LineString into segments of a speficied length.
    If a line segment ends up being shorter than the specified distance, it is merged with the segment before it.

    Arguments:
        linestring (Shapely LineString): Line to be cut into segments
        seg_length (numerical): The length of the segments

    Returns:
        lines (Shapely MultiLineString): A multilinestring consisting of the line segments.
    '''

    org_length = linestring.length

    no_segments = math.ceil(org_length / seg_length)

    start = 0
    end = seg_length
    lines = []

    for _ in range(no_segments):

        assert start != end

        l = substring(linestring, start, end)
      
        lines.append(l)

        start += seg_length
        end += seg_length
    
    # If the last segment is too short, merge it with the one before
    # Check that more than one line exist (to avoid cases where the line is too short to create multiple segments)
    if len(lines) > 1:
        for i, l in enumerate(lines):
            if l.length < seg_length/3:
                new_l = linemerge((lines[i-1], l))

                lines[i-1] = new_l

                del lines[i]

    lines = MultiLineString(lines)
    
    return lines


def _merge_multiline(line_geom):

    '''
    Convert a Shapely MultiLinestring into a Linestring
    
    Arguments:
        line_geom (Shapely LineString or MultiLineString): geometry to be merged
       
    Returns:
        line_geom (Shapely LineString): original geometry as LineString
    '''

    if line_geom.geom_type == 'MultiLineString':
        line_geom = linemerge(line_geom)
    
    assert line_geom.geom_type == 'LineString'

    return line_geom



def create_segment_gdf(gdf, segment_length):

    '''
    Takes a geodataframe with linestrings and converts it into shorter segments.

    Arguments:
        gdf (geodataframe): Geodataframe with linestrings to be converted to shorter segments
        segment_length (numerical): The length of the segments

    Returns:
        segments_gdf (geodataframe): New geodataframe with segments and new unique ids (seg_id)
    '''

    gdf['geometry'] = gdf['geometry'].apply(lambda x: _merge_multiline(x))
    assert gdf.geometry.geom_type.unique()[0] == 'LineString'

    gdf['geometry'] = gdf['geometry'].apply(lambda x: _get_segments(x, segment_length))
    segments_gdf = gdf.explode(index_parts=False, ignore_index=True)

    segments_gdf.dropna(subset=['geometry'],inplace=True)

    ids = []
    for i in range(1000, 1000+len(segments_gdf)):
        ids.append(i)

    segments_gdf['seg_id'] = ids
    assert len(segments_gdf['seg_id'].unique()) == len(segments_gdf)


    return segments_gdf


def clean_col_names(df):

    '''
    Remove upper-case letters and : from data with OSM tags
    Special characters like ':' can for example break with pd.query function

    Arguments:
        df (df/gdf): dataframe/geodataframe with OSM tag data

    Returns:
        df (df/gdf): the same dataframe with updated column names
    '''


    df.columns = df.columns.str.lower()

    df_cols = df.columns.to_list()

    new_cols = [c.replace(':','_') for c in df_cols]

    df.columns = new_cols

    return df

# TODO: Copy tests from CDQ folder