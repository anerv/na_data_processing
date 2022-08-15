#%%
import geopandas as gpd
import pandas as pd
import yaml
from src import db_functions as dbf
from src import graph_functions as gf
import json
import requests
from shapely.geometry.linestring import LineString
import polyline
import matplotlib.pyplot as plt
import json

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

    ref_id_col = parsed_yaml_file['geodk_id_col']
  
print('Settings loaded!')

#%%
connection = dbf.connect_pg(db_name, db_user, db_password)

get_geodk = 'SELECT * FROM geodk_bike_simple;'

get_osm = 'SELECT * FROM ox_edges;'

reference_data = gpd.GeoDataFrame.from_postgis(get_geodk, connection, geom_col='geometry' )

osm_data = gpd.GeoDataFrame.from_postgis(get_osm, connection, geom_col='geometry' )

assert len(reference_data) == len(reference_data[ref_id_col].unique())

# Dictionary for storing final matches between GeoDK and OSM
matches = {}

# To get enough points to perform map matching, original lines are segmentized
segments = gf.create_segment_gdf(reference_data, 10)

#%%
for ref_id in reference_data[ref_id_col].to_list():

    points = segments.loc[segments.fot_id==ref_id].copy()
    points['geometry'] = points.geometry.boundary
    points = points.explode(index_parts=False)
    points = points[['fot_id','geometry']]
    points = points.to_crs('EPSG:4326')
    points['lon'] = points.geometry.x
    points['lat'] = points.geometry.y
    points = points[['fot_id','lon','lat']]

    # VALHALLA request
    meili_coordinates = points.to_json(orient='records')
    meili_head = '{"shape":'
    meili_tail = ""","search_radius": 15, "max_search_radius": 30, "shape_match":"map_snap", "sigma_ z":"2", "turn_penalty_factor":"10",  "mode":"multimodal", "format":"osrm"}""" # "costing":"auto",
    meili_request_body = meili_head + meili_coordinates + meili_tail
    url = "http://localhost:8002/trace_attributes"
    headers = {'Content-type': 'application/json'}
    data = str(meili_request_body)
    r = requests.post(url, data=data, headers=headers)

    if r.status_code == 200:
        response_text = json.loads(r.text)
    else:
        print(r)

    # Get points representing nodes in matched OSM edges
    matched_df = pd.DataFrame()
    matched_df["lat"] = [i[0] for i in polyline.decode(response_text['shape'], 6)]
    matched_df["lon"] = [i[1] for i in polyline.decode(response_text['shape'], 6)]

    matched_df['id'] = matched_df.index

    # Convert to gdf
    matched_gdf = gpd.GeoDataFrame(
        matched_df, geometry=gpd.points_from_xy(matched_df.lon, matched_df.lat))
    matched_gdf.set_crs('EPSG:4326',inplace=True)

    # Get information about matched edges
    edges = pd.DataFrame.from_dict(response_text['edges'])
    edges = edges[['way_id','begin_shape_index','end_shape_index']]

    # Create line geometries for each edge/way
    start_points = edges.merge(matched_gdf, left_on='begin_shape_index', right_on='id', how='left')
    end_points = edges.merge(matched_gdf, left_on='end_shape_index', right_on='id', how='left')
    merged = pd.concat([start_points,end_points])

    grouped_edges = merged.groupby('way_id')
    lines = {}
    for way_id, edge in grouped_edges:

        l = LineString(edge.geometry.to_list())
        lines[way_id] = l

    lines_gdf = gpd.GeoDataFrame.from_dict(lines,orient='index',geometry=0)

    #lines_gdf.rename(columns={0:'geometry'},inplace=True)

    lines_gdf.set_crs('EPSG:4326',inplace=True).to_crs('EPSG:25832',inplace=True)

    lines_gdf['length'] = lines_gdf.geometry.length

    way_id = lines_gdf['length'].idxmax()

    matches[ref_id] = int(way_id)
    print('One edge matched!')

#%%
# Merge with reference and OSM data

final_matches_df = pd.DataFrame.from_dict(matches,orient='index')
final_matches_df.rename(columns={0:'way_id'},inplace=True)
final_matches_df.reset_index(inplace=True)
final_matches_df.rename({'index':ref_id_col},inplace=True,axis=1)

matched_geodk = reference_data.merge(final_matches_df,on=ref_id_col,how='inner')

matched_osm = osm_data.merge(final_matches_df, left_on='osmid',right_one='way_id',how='inner')

assert len(matches) == len(matched_geodk)

assert len(matches) == len(matched_osm)

#%%
fig, ax = plt.subplots(figsize=(30,30))
matched_geodk.plot(ax=ax, color='red')
matched_osm.plot(ax=ax,color='blue')
#%%
# Save data on matches
with open('matches.json', 'w') as fp:
    json.dump(matches, fp)

if use_postgres:

    print('Saving data to PostgreSQL!')

    connection = dbf.connect_pg(db_name, db_user, db_password)

    engine = dbf.connect_alc(db_name, db_user, db_password, db_port=db_port)

    final_matches_df.to_sql('feature_matches', engine)

# %%
