'''
Script for matching road networks
Method assumes a simplified version of public dataset - that edges generally are longer or as long as OSM??
Maybe use the OSM-no bike to speed up efficiency?
'''
# TODO: Docs
#%%
import geopandas as gpd
import pandas as pd
import numpy as np
import yaml
from src import db_functions as dbf
from src import geometric_functions as gf
from shapely.ops import nearest_points, split, linemerge
from shapely.geometry import Point, MultiPoint, LineString
from scipy.spatial.distance import directed_hausdorff
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

print(f'Number of rows in osm_edge table: {len(osm_edges)}')
print(f'Number of rows in geodk_bike table: {len(geodk_bike)}')
print(f'Number of rows in grid table: {len(grid)}')

#%%
# For analysing larger areas - do a grid by grid matching
env = gpd.GeoDataFrame()
env.at[0,'geometry'] = geodk_bike.unary_union.envelope
env = env.set_crs(crs) 

clipped_grid = gpd.clip(grid, env)

#%%
# TODO Change to taking OSM as starting point???

# Create buffered geometries from all of GeoDK geometries - maintain index/link to original features

geodk_buff = geodk_bike.copy(deep=True)

geodk_buff.geometry = geodk_bike.geometry.buffer(distance=7)

osm_sindex = osm_edges.sindex

#%%
osm_matches = pd.DataFrame(index=geodk_buff.index)

osm_matches['matches_index'] = None
osm_matches['matches_osmid'] = None
osm_matches['fot_id'] = None

for index, row in geodk_buff.iterrows():
   
    poly = row['geometry']
    possible_matches_index = list(osm_sindex.intersection(poly.bounds))
    possible_matches = osm_edges.iloc[possible_matches_index]
    precise_matches = possible_matches[possible_matches.intersects(poly)]
    precise_matches_index = list(precise_matches.index)
    precise_matches_id = list(precise_matches.osmid)
    osm_matches.at[index, 'matches_index'] = precise_matches_index
    osm_matches.at[index, 'matches_osmid'] = precise_matches_id
    osm_matches.at[index, 'fot_id'] = row.fot_id
  
#%%
# Now we have all osm lines intersecting the geodk bike buffer

osm_matched = []
geodk_matched = []
geodk_not_matched = []
geodk_part_matched = gpd.GeoDataFrame(index= geodk_bike.index, columns=['geometry'], crs=crs)
geodk_several_matches = pd.DataFrame(index=geodk_bike.index, columns=['fotid','osmid'])
final_matches = gpd.GeoDataFrame(columns=['geodk_fotid','osmid','geometry'], crs=crs)

angular_threshold = 20 # threshold in degress
hausdorff_threshold = 12 # threshold in meters

percent_removed_threshold = 30 # in percent
meters_removed_threshold = 5 # in meters

for index, row in osm_matches.iterrows(): # OBS Use something else than iterrows??

    # If no matches exist at all, continue to next GeoDk geometry
    if len(row.matches_index) < 1:

        geodk_not_matched.append(row.fot_id)

        continue

    else:

        osm_df = osm_edges[['osmid','highway','name','geometry']].iloc[row.matches_index].copy(deep=True)

        osm_df['hausdorff_dist'] = None
        osm_df['angle'] = None

        geodk_edge = geodk_bike.loc[index].geometry

        if geodk_edge.geom_type == 'MultiLineString':
            geodk_edge = linemerge(geodk_edge)
        
        #Dataframe for storing how much is clipped with the different OSM matches
        clipped_edges = gpd.GeoDataFrame(index=osm_df.index, columns=['clipped_length','meters_removed','percent_removed','fot_id','geometry'], crs=crs)
        clipped_edges['geometry'] = None

        for i, r in osm_df.iterrows():

            # i here is OSM index!

            osm_edge = r.geometry

            osm_start_node = Point(osm_edge.coords[0])
            osm_end_node = Point(osm_edge.coords[-1])

            # Get nearest point on GeoDK geometry to start and end nodes of OSM match
            queried_point, nearest_point_start = nearest_points(osm_start_node, geodk_edge)
            queried_point, nearest_point_end = nearest_points(osm_end_node, geodk_edge)
    
            # Clip GeoDK geometry with nearest points
            clip_points = MultiPoint([nearest_point_start, nearest_point_end])

            #Check if line is clipped at all - if not

            if len(split(geodk_edge, clip_points).geoms) == 1:
                # Line is not clipped - they are either completely identical or perpendicular
                clipped_geodk_edge = split(geodk_edge, clip_points).geoms[0]

            else:
                clipped_geodk_edge = split(geodk_edge, clip_points).geoms[1]
                meters_removed = geodk_edge.length - clipped_geodk_edge.length
                percent_removed = meters_removed * 100 / geodk_edge.length
                clipped_edges.at[i,'clipped_length'] = clipped_geodk_edge.length
                clipped_edges.at[i,'meters_removed'] = meters_removed
                clipped_edges.at[i,'percent_removed'] = percent_removed
                clipped_edges.at[i, 'fot_id'] = index
                clipped_edges.at[i, 'geometry'] = gf.get_geom_diff(geodk_edge, clipped_geodk_edge)


            # If length of clipped GeoDK is very small it indicates that the OSM edge is perpendicular with the GeoDK edge and thus not a correct match
            if clipped_geodk_edge.length < 2:
                continue

            # Compute Hausdorff distance (max distance between two lines)
            osm_coords = list(osm_edge.coords)
            geodk_coords = list(clipped_geodk_edge.coords)
            hausdorff_dist = max(directed_hausdorff(osm_coords, geodk_coords)[0], directed_hausdorff(geodk_coords, osm_coords)[0])
            
            osm_df.at[i, 'hausdorff_dist'] = hausdorff_dist

            # Angular difference
            angle_deg = gf.get_angle(osm_edge, geodk_edge)
            osm_df.at[i, 'angle'] = angle_deg

        # Find potential matches out of all matches for this geodk geometry - i.e. filter out matches that are not within thresholds
        potential_matches = osm_df[ (osm_df.angle < angular_threshold) & (osm_df.hausdorff_dist < hausdorff_threshold)]

        if len(potential_matches) == 0:

            geodk_not_matched.append(index)

            continue
        
        elif len(potential_matches) == 1:

            geodk_matched.append(index)
            osm_matched.append(potential_matches.index.values[0])

            final_matches.at[index,'geodk_fotid'] = row.fot_id
            final_matches.at[index, 'osmid'] = potential_matches.osmid.values[0]
            final_matches.at[index, 'geometry'] = potential_matches.geometry.values[0]

            # First check if osmid have been added to clipped edges df at all - only happens if the geodk edge is actually clipped
            if clipped_edges.loc[potential_matches.index[0], 'percent_removed'] > percent_removed_threshold and clipped_edges.loc[potential_matches.index[0], 'meters_removed'] > meters_removed_threshold:
                
                #Save removed geometries
                removed_edge = clipped_edges.loc[potential_matches.index[0], ['geometry']].values[0]
                geodk_part_matched.at['geometry'] = removed_edge
        
        else:

            # Get match(es) with smallest Hausdorff distance and angular tolerance
            osm_df['hausdorff_dist'] = pd.to_numeric(osm_df['hausdorff_dist'] )
            osm_df['angle'] = pd.to_numeric(osm_df['angle'])
            
            #best_matches = osm_df[['hausdorff_dist','angle']].min()
            best_matches_index = osm_df[['hausdorff_dist','angle']].idxmin()
            best_matches = osm_df.loc[best_matches_index].copy(deep=True)
            
            best_matches = best_matches[~best_matches.index.duplicated(keep='first')]

            if len(best_matches) == 1:

                geodk_matched.append(index)
                osm_matched.append(best_matches.index.values[0])

                final_matches.at[index,'geodk_fotid'] = row.fot_id
                final_matches.at[index, 'osmid'] = best_matches.osmid.values[0]
                final_matches.at[index, 'geometry'] = best_matches.geometry.values[0]

                if clipped_edges.loc[best_matches.index[0], 'percent_removed'] > percent_removed_threshold and clipped_edges.loc[best_matches.index[0], 'meters_removed'] > meters_removed_threshold:
                
                    #Save removed geometries
                    removed_edge = clipped_edges.loc[best_matches.index[0], ['geometry']].values[0]
                    geodk_part_matched.at['geometry'] = removed_edge

            elif len(best_matches) > 1:

                # Do a couple of more tests - what is the issue??
                # Take the one with the smallest hausdorff distance

                geodk_several_matches.at[index, 'fotid'] = row.fot_id
                geodk_several_matches.at[index, 'osmid'] = list(best_matches.osmid)
                #geodk_several_matches.append(row.fot_id)

                break

    
                    # If more than one - save the best 2 candidates? The one with smallest distance and the one with smallest angle?
                    # Or do a new filtering with a smaller angular threshold and smaller dist (e.g. 10 meters and 10 degrees)
                    # Do one at a time maybe loop untill only one?

                    # OBS! Check how much of geodk geometry has been clipped for this match!
                    # Save match between osm and geodk edges
    
    # TODO: For partially matched - if length is above threshold do a new iteration - but without including already matched OSM edges

#%%
# Look at how many GeoDK are unmatched and how many are partially matched

# Unmatched - maybe simply add to network???

# Partially matched - if length is above threshold do a new iteration - but without including already matched OSM edges
# And only include removed geometries!!

#%%
# Transfer GeoDk cycling attribute to OSM
        # At this point - look at clipped df - if more than XXX meters and XXX percent has been clipped

        # Question - what about 'unmatched' GeoDk cycling infra - either no match or segments clipped away when clipping to OSM extent?
        # Find a way of quantifying the problem/adding it to dataset
        # Use list of matched GeoDK edges and create a df in loop for all geometries - save clipped percent etc for final choice/Match with osm
        # Also save the unmatched geometries?? could be small lines on each side, not that relevant

        
    # Find a way to save results / transfer to OSM data

    # First I took GeoDK as a starting point - now I am interested in OSM
    # Clip GeoDK with each matched feature
        # What happens if OSM is much longer..?
        # Find Hausdorff distance based on snapped points then
        # See if it's parallel
        # Chose the closests?

    
    
    # Which one do I want to snap? I want to find the OSM line that matches the GeoDK bike! 
    # ....start and end points to GeoDK geometry
    # Check how much of the line is removed!!! If under XXX meter/percent, don't clip
        #Or maybe clip for hausdorff computation, but keep original geometry
    # Extract that as the new GeoDK element
    # Compute Hausdorff distance

    #Decide on best match?

    # How to get info back in original format?


    #Compute length differences between GeoDK and all matches
    #GeoDK can be longer than OSM, but OSM cannot be longer than GeoDK
    #Compute Hausdorff distances

    

#%%
# Upload result to DB
engine = dbf.connect_alc(db_name, db_user, db_password, db_port=db_port)

dbf.to_postgis(geodk_buff, 'geodk_buff', engine)

connection.close()

#%%
# Angular difference
#45
line1 = LineString([[2,2],[2,10]])
line2 = LineString([[4,11],[3,3]])

#%%
arr1 = np.array(line1.coords)
arr1 = arr1[1] - arr1[0]

arr2 = np.array(line2.coords)
arr2 = arr2[1] - arr2[0]

angle = np.math.atan2(np.linalg.det([arr1,arr2]),np.dot(arr1,arr2))
angle_deg = abs(np.degrees(angle))

print(angle_deg)
# %%
def get_angle(linestring1, linestring2):

    arr1 = np.array(linestring1.coords)
    arr1 = arr1[1] - arr1[0]

    arr2 = np.array(linestring2.coords)
    arr2 = arr2[1] - arr2[0]

    angle = np.math.atan2(np.linalg.det([arr1,arr2]),np.dot(arr1,arr2))
    angle_deg = abs(np.degrees(angle))

    if angle_deg > 90:
        angle_deg = 180 - angle_deg

    return angle_deg
    
# %%
test = get_angle(line1, line2)
# %%
print(test)
# %%
