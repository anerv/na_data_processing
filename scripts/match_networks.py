'''
Script for matching road networks
'''
# TODO: Docs
#%%

import geopandas as gpd
import yaml
from src import db_functions as dbf
import pandas as pd
#%%

with open(r'config.yml') as file:
    parsed_yaml_file = yaml.load(file, Loader=yaml.FullLoader)

    crs = parsed_yaml_file['CRS']

    db_name = parsed_yaml_file['db_name']
    db_user = parsed_yaml_file['db_user']
    db_password = parsed_yaml_file['db_password']
    db_host = parsed_yaml_file['db_host']
    db_port = parsed_yaml_file['db_port']
  
    
print('Settings loaded!')

#%%
connection = dbf.connect_pg(db_name, db_user, db_password)

get_osm = 'SELECT * FROM osm_edges;'

get_geodk = 'SELECT * FROM geodk_bike;'

get_grid = 'SELECT * FROM grid;'

osm_edges = gpd.GeoDataFrame.from_postgis(get_osm, connection, geom_col='geometry' )

geodk_bike = gpd.GeoDataFrame.from_postgis(get_geodk, connection, geom_col='geometry' )

grid = gpd.GeoDataFrame.from_postgis(get_grid, connection, geom_col='geometry')

assert osm_edges.crs == crs
assert geodk_bike.crs == crs
assert grid.crs == crs

print(f'Number of rows in osm_no_bike table: {len(osm_edges)}')
print(f'Number of rows in geodk_bike table: {len(geodk_bike)}')
print(f'Number of rows in grid table: {len(grid)}')

#%%
# For analysing larger areas - do a grid by grid matching
env = gpd.GeoDataFrame()
env.at[0,'geometry'] = geodk_bike.unary_union.envelope
env = env.set_crs(crs)

clipped_grid = gpd.clip(grid, env)
#%%
# Create buffered geometries from all of GeoDK geometries - maintain index/link to original features

geodk_buff = geodk_bike.copy(deep=True)

geodk_buff.geometry = geodk_bike.geometry.buffer(distance=10)

osm_sindex = osm_edges.sindex
#%%
# Not a great solution at this step - keeps only the nearest feature
#matches = gpd.sjoin_nearest(geodk_bike, osm_edges, how='inner', max_distance=10, distance_col='dist')

#%%
osm_matches = pd.DataFrame(index=geodk_buff.index)

osm_matches['matches_index'] = None
osm_matches['matches_osmid'] = None
osm_matches['fot_id'] = None


for index, row in geodk_buff.iterrows():
   
    poly = row['geometry']
    possible_matches_index = list(osm_sindex.intersection(poly.bounds))
    #possible_matches_index = list(osm_sindex.query_bulk(poly, predicate='intersects')[1])
    
    possible_matches = osm_edges.iloc[possible_matches_index]
    precise_matches = possible_matches[possible_matches.intersects(poly)]
    precise_matches_index = list(precise_matches.index)
    precise_matches_id = list(precise_matches.osmid)
    osm_matches.at[index, 'matches_index'] = precise_matches_index
    osm_matches.at[index, 'matches_osmid'] = precise_matches_id
    osm_matches.at[index, 'fot_id'] = row.fot_id
  

#%%
# Now I have all osm lines intersecting the geodk bike buffer
# for each row:
# get osm geometries through match index
# Find the best match:
    #Compute length of all features
    #Compute Hausdorff distance for all matches

geodk_bike['length'] = geodk_bike.geometry.length

osm_edges['length'] = osm_edges.geometry.length

for index, row in osm_matches.iterrows():
    osm_df = osm_edges.loc[row.matches]
    

#%%
# Upload result to DB
engine = dbf.connect_alc(db_name, db_user, db_password, db_port=db_port)

dbf.to_postgis(geodk_buff, 'geodk_buff', engine)

connection.close()

#%%
from shapely.geometry import Point

poly2 = geodk_buff['geometry'].loc[geodk_buff['fot_id']==1087380287]

point1 = Point(poly2.bounds['minx'].values, poly2.bounds['miny'].values)
point2 = Point(poly2.bounds['maxx'].values, poly2.bounds['maxy'].values)

points=gpd.GeoDataFrame(geometry=gpd.points_from_xy([point1.x, point2.x],[point1.y, point2.y]))

buffer = geodk_buff.loc[geodk_buff['fot_id']==1087380287]
line = osm_edges.loc[osm_edges.osmid==26927690]

ax = buffer.plot(figsize=(10,10))
line.plot(ax=ax, color='red')
points.plot(ax=ax, color='green')
#%%

# %%
