#%%

import geopandas as gpd
import pandas as pd
from sklearn.feature_selection import SelectorMixin
import yaml
from src import db_functions as dbf
import json
import requests
from shapely.geometry.linestring import LineString
from pyproj import Geod
import polyline
import matplotlib.pyplot as plt
#from decode_functions import decode


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
from shapely.ops import linemerge, MultiLineString, substring
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

    # TODO: Write docs

    # Convert a Shapely MultiLinestring into a Linestring

    if line_geom.geom_type == 'MultiLineString':
        line_geom = linemerge(line_geom)

    return line_geom

##############################

def create_segment_gdf(org_gdf, segment_length):

    '''
    Takes a geodataframe with linestrings and converts it into shorter segments.

    Arguments:
        gdf (geodataframe): Geodataframe with linestrings to be converted to shorter segments
        segment_length (numerical): The length of the segments

    Returns:
        segments_gdf (geodataframe): New geodataframe with segments and new unique ids (seg_id)
    '''

    gdf = org_gdf.copy()
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
#%%
connection = dbf.connect_pg(db_name, db_user, db_password)

get_geodk = 'SELECT * FROM geodk_bike_simple;'

reference_data = gpd.GeoDataFrame.from_postgis(get_geodk, connection, geom_col='geometry' )
#%%
#selection = reference_data.loc[reference_data.vejkode=='8008'].copy()

selection = reference_data.loc[reference_data.fot_id=='1087377452'].copy() # 1087281433
#%%
segments = create_segment_gdf(selection, 10)
#%%
points = segments.copy()
points['geometry'] = points.geometry.boundary
points = points.explode(index_parts=False)
points = points[['fot_id','geometry']]
points = points.to_crs('EPSG:4326')
points['lon'] = points.geometry.x
points['lat'] = points.geometry.y
points = points[['fot_id','lon','lat']]
# %%
'''
mkdir custom_files
wget -O custom_files/denmark-latest.osm.pbf https://download.geofabrik.de/europe/denmark-latest.osm.pbf
docker run --name valhalla_gis-ops -p 8002:8002 -v $PWD/custom_files:/custom_files gisops/valhalla:latest
'''
#%% VALHALLA REQUEST
# TODO: format what is included in the response - I just want edge ids?
# write code for doing it for several rows - saving the link between fotid and the potentially several way ids its been matched to
meili_coordinates = points.to_json(orient='records')
meili_head = '{"shape":'
meili_tail = ""","search_radius": 15, "max_search_radius": 30, "shape_match":"map_snap", "sigma_ z":"2", "turn_penalty_factor":"10", "costing":"auto", "mode":"auto", "format":"osrm"}"""
meili_request_body = meili_head + meili_coordinates + meili_tail
url = "http://localhost:8002/trace_attributes"
headers = {'Content-type': 'application/json'}
data = str(meili_request_body)
r = requests.post(url, data=data, headers=headers)
#%%
if r.status_code == 200:
    response_text = json.loads(r.text)
    # search_1 = response_text.get('matchings')
    # search_2 = dict(search_1[0])
    # polyline6 = search_2.get('geometry')
    # search_3 = response_text.get('tracepoints')
else:
    print(r)
#%%
matched_df = pd.DataFrame()
matched_df["lat"] = [i[0] for i in polyline.decode(response_text['shape'], 6)]
matched_df["lon"] = [i[1] for i in polyline.decode(response_text['shape'], 6)]

matched_df['id'] = matched_df.index

matched_df.head()

matched_gdf = gpd.GeoDataFrame(
    matched_df, geometry=gpd.points_from_xy(matched_df.lon, matched_df.lat))
matched_gdf.set_crs('EPSG:4326',inplace=True)

fig, ax = plt.subplots()
matched_gdf.plot(ax=ax, color='purple')
selection.to_crs(matched_gdf.crs).plot(ax=ax)

'''
Write function that creates data with LINES, wayid

Project to other CRS

Compute length

getting points from shapes is easy

'''

#%%
edges = pd.DataFrame.from_dict(response_text['edges'])

edges = edges[['way_id','begin_shape_index','end_shape_index']]

start_points = edges.merge(matched_gdf, left_on='begin_shape_index', right_on='id', how='left')
end_points = edges.merge(matched_gdf, left_on='end_shape_index', right_on='id', how='left')

merged = pd.concat([start_points,end_points])
#%%
lines = merged.groupby('way_id')['geometry'].apply( lambda x: LineString(x.to_list())) # TODO: fix line construction
lines_gdf = gpd.GeoDataFrame(lines,geometry='geometry')
lines_gdf.set_crs('EPSG:4326').to_crs('EPSG:25832').length


#%%
start_points.rename(columns={'lat':'lat_start','lon':'lon_start'},inplace=True)
end_points.rename(columns={'lat':'lat_end','lon':'lon_end'},inplace=True)
merged = start_points.merge(end_points, how='inner', on='way_id')
merged['geometry'] = 
# rename lat lon to indicate beginning coord

# merge end points

# create line geometry

# compute lengths

# take

#%%
# def decode(encoded):
#   inv = 1.0 / 1e6
#   decoded = []
#   previous = [0,0]
#   i = 0
#   #for each byte
#   while i < len(encoded):
#     #for each coord (lat, lon)
#     ll = [0,0]
#     for j in [0, 1]:
#       shift = 0
#       byte = 0x20
#       #keep decoding bytes until you have this coord
#       while byte >= 0x20:
#         byte = ord(encoded[i]) - 63
#         i += 1
#         ll[j] |= (byte & 0x1f) << shift
#         shift += 5
#       #get the final value adding the previous offset and remember it for the next
#       ll[j] = previous[j] + (~(ll[j] >> 1) if ll[j] & 1 else (ll[j] >> 1))
#       previous[j] = ll[j]
#     #scale by the precision and chop off long coords also flip the positions so
#     #its the far more standard lon,lat instead of lat,lon
#     decoded.append([float('%.6f' % (ll[1] * inv)), float('%.6f' % (ll[0] * inv))])
#   #hand back the list of coordinates
#   return decoded
# #%%
# lst_MapMatchingRoute = LineString(decode(polyline6))
# gdf_MapMatchingRoute_linestring = gpd.GeoDataFrame(geometry=[lst_MapMatchingRoute], crs=4326)
# gdf_MapMatchingRoute_points_temp = gdf_MapMatchingRoute_linestring.apply(lambda x: [y for y in x['geometry'].coords], axis=1)
# gdf_MapMatchingRoute_points = gpd.GeoDataFrame(geometry=gpd.points_from_xy([a_tuple[0] for a_tuple in gdf_MapMatchingRoute_points_temp[0]], [a_tuple[1] for a_tuple in gdf_MapMatchingRoute_points_temp[0]]), crs=4326)
# gdf_MapMatchingRoute = gpd.GeoDataFrame(pd.concat([gdf_MapMatchingRoute_linestring, gdf_MapMatchingRoute_points], ignore_index=True))
# df_mapmatchedGPS_points = pd.DataFrame(list([d['location'] for d in search_3 if 'location' in d]) , columns=['lon', 'lat'])
# gdf_mapmatchedGPS_points = gpd.GeoDataFrame(geometry=gpd.points_from_xy(df_mapmatchedGPS_points['lon'], df_mapmatchedGPS_points['lat']), crs=4326)
# # %%
