'''
Script for matching road networks.
Method assumes a simplified version of public dataset and a non-simplified version of OSM (i.e. that edges in the reference dataset usually are as long or longer than OSM).
Use case is situations where a specific attribute is poorly defined in OSM, and the script is thus developed for a situation where it is assumed that most core road geometries are present in both data sets.
But it is also possible to use for identifying edges in the reference dataset with no match in OSM or vice-versa.

'''
# TODO: Docs
# TODO; Remove hardcoding of GeoDK - rename to reference
# TODO: Convert to function
# TODO: For partially matched - if length is above threshold do a new iteration - but without including already matched OSM edges
# TODO: Functionality for matching to OSM data
# TODO: Functionality for doing analysis grid by grid
# TODO: Compute how many have been matched and how many have been updated
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

reference = gpd.GeoDataFrame.from_postgis(get_geodk, connection, geom_col='geometry' )

grid = gpd.GeoDataFrame.from_postgis(get_grid, connection, geom_col='geometry')

assert osm_edges.crs == crs
assert reference.crs == crs
assert grid.crs == crs

print(f'Number of rows in osm_edge table: {len(osm_edges)}')
print(f'Number of rows in reference table: {len(reference)}')
print(f'Number of rows in grid table: {len(grid)}')

#%%
'''
# For analysing larger areas - do a grid by grid matching
env = gpd.GeoDataFrame()
env.at[0,'geometry'] = reference.unary_union.envelope
env = env.set_crs(crs) 

clipped_grid = gpd.clip(grid, env)
'''

#%%
# Define name of id col in ref dataset
ref_id = 'fot_id'

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
    matches = pd.DataFrame(index=reference_buff.index, columns=['matches_index','matches_osmid',col_ref_id])

    for index, row in reference_buff.iterrows():
        # The function makes use of a spatial index to first identify potential matches before finding exact matches
   
        buffer = row['geometry']

        possible_matches_index = list(osm_sindex.intersection(buffer.bounds))

        possible_matches = osm_edges.iloc[possible_matches_index]

        precise_matches = possible_matches[possible_matches.intersects(buffer)]

        precise_matches_index = list(precise_matches.index)

        precise_matches_id = list(precise_matches.osmid)

        matches.at[index, 'matches_index'] = precise_matches_index
        matches.at[index, 'matches_osmid'] = precise_matches_id

        matches.at[index, ref_id] = row[ref_id]

    return matches

#%%
matches = find_matches_buffer(reference_data=reference, osm_data=osm_edges, col_ref_id=ref_id, dist=7)

# Now we have all osm lines intersecting the reference buffer

#%%
def find_exact_matches(matched_data, osm_data, reference_data, angular_threshold=20, hausdorff_threshold=12, pct_removed_threshold=20, meters_removed_threshold=5):
    
    osm_matched = []
    reference_matched = []
    ref_not_matched = []
    ref_part_matched = gpd.GeoDataFrame(index= reference.index, columns=['geometry'], crs=crs)
    ref_several_matched = pd.DataFrame(index=reference.index, columns=[ref_id,'osmid'])
    final_matches = gpd.GeoDataFrame(columns=[ref_id,'osmid','geometry'], crs=crs)

    pass

#%%
osm_matched = []
reference_matched = []
ref_not_matched = []
ref_part_matched = gpd.GeoDataFrame(index= reference.index, columns=['geometry'], crs=crs)
ref_several_matched = pd.DataFrame(index=reference.index, columns=[ref_id,'osmid'])
final_matches = gpd.GeoDataFrame(columns=[ref_id,'osmid','geometry'], crs=crs)

angular_threshold = 20 # threshold in degress
hausdorff_threshold = 12 # threshold in meters

pct_removed_threshold = 20 # in pct
meters_removed_threshold = 5 # in meters

for index, row in matches.iterrows(): # TODO: Use something else than iterrows?


    # If no matches exist at all, continue to next reference geometry and add the reference feature as unmatched
    if len(row.matches_index) < 1:

        ref_not_matched.append(row[ref_id])

        continue

    else:

        # Get the original geometry for the reference feature
        ref_edge = reference.loc[index].geometry

        # Some steps will not work for MultiLineString - convert those to LineString
        # This step assumes that MultiLineString do not have gaps!
        if ref_edge.geom_type == 'MultiLineString':
            ref_edge = linemerge(ref_edge)

        # Get the original geometries that intersected this reference geometry's buffer
        osm_df = osm_edges[['osmid','highway','name','geometry']].iloc[row.matches_index].copy(deep=True)

        osm_df['hausdorff_dist'] = None
        osm_df['angle'] = None
        
        #Dataframe for storing how much is removed from the reference geometry with the different OSM matches
        clipped_edges = gpd.GeoDataFrame(index=osm_df.index, columns=['clipped_length','meters_removed','pct_removed',ref_id,'geometry'], crs=crs)
        clipped_edges['geometry'] = None


        # Loop through all matches and compute how good of a match they are
        for i, r in osm_df.iterrows():
            # i here is OSM index!

            osm_edge = r.geometry # Get the geometry of this specific matched OSM edge

            osm_start_node = Point(osm_edge.coords[0])
            osm_end_node = Point(osm_edge.coords[-1])

            # Get nearest point on reference geometry to start and end nodes of OSM match
            queried_point, nearest_point_start = nearest_points(osm_start_node, ref_edge)
            queried_point, nearest_point_end = nearest_points(osm_end_node, ref_edge)
    
            # Clip reference geometry with nearest points
            clip_points = MultiPoint([nearest_point_start, nearest_point_end])

            # Check if line is clipped at all by checking how many geometries are returned by split function
            if len(split(ref_edge, clip_points).geoms) == 1:

                # Line is not clipped - they are either completely identical OR perpendicular
                # Continue with the unclipped edge
                clipped_ref_edge = split(ref_edge, clip_points).geoms[0]

            else:
                # Line is clipped

                if len(split(ref_edge, clip_points).geoms) == 2:
                    clipped_ref_edge = split(ref_edge, clip_points).geoms[0] # This happens if one of the clip points are identical to a start or end point on the line
                
                elif len(split(ref_edge, clip_points).geoms) == 3: 
                    clipped_ref_edge = split(ref_edge, clip_points).geoms[1] # Keep the clipped geometry placed between the two points
                
                meters_removed = ref_edge.length - clipped_ref_edge.length
                pct_removed = meters_removed * 100 / ref_edge.length

                clipped_edges.at[i,'clipped_length'] = clipped_ref_edge.length
                clipped_edges.at[i,'meters_removed'] = meters_removed
                clipped_edges.at[i,'pct_removed'] = pct_removed
                clipped_edges.at[i, ref_id] = index

                clipped_edges.at[i, 'geometry'] = gf.get_geom_diff(ref_edge, clipped_ref_edge)


            # If length of clipped reference edge is very small it indicates that the OSM edge is perpendicular with the reference edge and thus not a correct match
            if clipped_ref_edge.length < 2:
                continue

            # Compute Hausdorff distance (max distance between two lines)
            osm_coords = list(osm_edge.coords)
            ref_coords = list(clipped_ref_edge.coords)
            hausdorff_dist = max(directed_hausdorff(osm_coords, ref_coords)[0], directed_hausdorff(ref_coords, osm_coords)[0])
            
            osm_df.at[i, 'hausdorff_dist'] = hausdorff_dist

            # Angular difference
            angle_deg = gf.get_angle(osm_edge, ref_edge)
            osm_df.at[i, 'angle'] = angle_deg

        # Find potential matches out of all matches for this referehce geometry - i.e. filter out matches that are not within thresholds
        potential_matches = osm_df[ (osm_df.angle < angular_threshold) & (osm_df.hausdorff_dist < hausdorff_threshold)]

        if len(potential_matches) == 0:

            ref_not_matched.append(index)

            continue
        
        elif len(potential_matches) == 1:

            reference_matched.append(index)
            osm_matched.append(potential_matches.index.values[0])

            final_matches.at[index,'ref_id'] = row.fot_id
            final_matches.at[index, 'osmid'] = potential_matches.osmid.values[0]
            final_matches.at[index, 'geometry'] = potential_matches.geometry.values[0]

            # First check if osmid have been added to clipped edges df at all - only happens if the reference edge is actually clipped
            if clipped_edges.loc[potential_matches.index[0], 'pct_removed'] > pct_removed_threshold and clipped_edges.loc[potential_matches.index[0], 'meters_removed'] > meters_removed_threshold:
                
                #Save removed geometries
                removed_edge = clipped_edges.loc[potential_matches.index[0], ['geometry']].values[0]
                ref_part_matched.at['geometry'] = removed_edge
        
        else:

            # Get match(es) with smallest Hausdorff distance and angular tolerance
            osm_df['hausdorff_dist'] = pd.to_numeric(osm_df['hausdorff_dist'] )
            osm_df['angle'] = pd.to_numeric(osm_df['angle'])
            
            #best_matches = osm_df[['hausdorff_dist','angle']].min()
            best_matches_index = osm_df[['hausdorff_dist','angle']].idxmin()
            best_matches = osm_df.loc[best_matches_index].copy(deep=True)
            
            best_matches = best_matches[~best_matches.index.duplicated(keep='first')]

            if len(best_matches) == 1:

                reference_matched.append(index)
                osm_matched.append(best_matches.index.values[0])

                final_matches.at[index,'ref_id'] = row.fot_id
                final_matches.at[index, 'osmid'] = best_matches.osmid.values[0]
                final_matches.at[index, 'geometry'] = best_matches.geometry.values[0]

                if clipped_edges.loc[best_matches.index[0], 'pct_removed'] > pct_removed_threshold and clipped_edges.loc[best_matches.index[0], 'meters_removed'] > meters_removed_threshold:
                
                    #Save removed geometries
                    removed_edge = clipped_edges.loc[best_matches.index[0], ['geometry']].values[0]
                    ref_part_matched.at['geometry'] = removed_edge

            elif len(best_matches) > 1:

                # Do a couple of more tests - what is the issue??
                # Take the one with the smallest hausdorff distance!
                # If Hausdorff distance is identical, add to list of several matches

                ref_several_matched.at[index, ref_id] = row[ref_id]
                ref_several_matched.at[index, 'osmid'] = list(best_matches.osmid)
                #ref_several_matched.append(row.fot_id)

                break

    
                    # If more than one - save the best 2 candidates? The one with smallest distance and the one with smallest angle?
                    # Or do a new filtering with a smaller angular threshold and smaller dist (e.g. 10 meters and 10 degrees)
                    # Do one at a time maybe loop untill only one?

                    # OBS! Check how much of reference geometry has been clipped for this match!
                    # Save match between osm and reference edges
    
  

#%%
# Look at how many reference features are unmatched and how many are partially matched

# Unmatched - maybe simply add to network???

# Partially matched - if length is above threshold do a new iteration - but without including already matched OSM edges
# And only include removed geometries!!

#%%
# Transfer GeoDk cycling attribute to OSM
        # At this point - look at clipped df - if more than XXX meters and XXX pct has been clipped

        # Question - what about 'unmatched' GeoDk cycling infra - either no match or segments clipped away when clipping to OSM extent?
        # Find a way of quantifying the problem/adding it to dataset
        # Use list of matched GeoDK edges and create a df in loop for all geometries - save clipped pct etc for final choice/Match with osm
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
    # Check how much of the line is removed!!! If under XXX meter/pct, don't clip
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
clip_points = MultiPoint([Point(2,2), Point(7,7)])

line2 = LineString( [ (1,1), (10,10) ] )
line1 = LineString( [ (2,2),(7,7) ] )
#%%
test = split( line2, clip_points)
# %%
def get_geom_diff2(geom1, geom2):

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
#%%

geoms1 = [line2]
geoms2 = [line1]

geodf1 = gpd.GeoDataFrame(geometry=geoms1)

geodf2 = gpd.GeoDataFrame(geometry=geoms2)

geom_diff = geodf1.difference(geodf2).values[0]
# %%
