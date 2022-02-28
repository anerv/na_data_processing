'''
Script for matching road networks.
Method assumes a simplified version of public dataset and a non-simplified version of OSM (osm_i.e. that edges in the reference dataset usually are as long or longer than OSM).
Use case is situations where a specific attribute is poorly defined in OSM, and the script is thus developed for a situation where it is assumed that most core road geometries are present in both data sets.
But it is also possible to use for identifying edges in the reference dataset with no match in OSM or vice-versa.

'''
# TODO: Docs
# TODO: Convert to function
# TODO: For partially matched - if length is above threshold do a new iteration - but without including already matched OSM edges
# TODO: Functionality for matching to OSM data
# TODO: Functionality for doing analysis grid by grid
# TODO: Compute how many have been matched and how many have been updated
# TODO: Function for adding unmatched to dataset?


# Fix issues with some that are unmatched

# Convert to function
# Work on how to do new iteration - how many times? As long as new edges are matched? Buffer does not need to be repeated(?) 
# Should more than one OSM-match be allowed - yes, but not more than two?
# Next iteration should exclude those GeoDk that have been matched (those that are in final matches AND not in part mathces)
# Next interation should thus consist of those partially matched - so I should repeat buffer! And partially matched should be reset for every iteration - but not fully matched or unmatched

# Method that, as long as partially matched decreases, keeps going? Or which 


# When done, drop na from several matched and part matched

#%%
import geopandas as gpd
import pandas as pd
import yaml
from src import db_functions as dbf
from src import geometric_functions as gf
from shapely.ops import nearest_points, split, linemerge, snap
from shapely.geometry import Point, MultiPoint, LineString
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

get_osm = '''SELECT * FROM osm_edges WHERE highway IN ('residential', 'service', 'primary', 'tertiary',
       'tertiary_link', 'secondary', 'cycleway', 'path', 'living_street',
       'unclassified', 'primary_link', 'motorway_link', 'motorway', 'track',
       'secondary_link', 'pathway', 'trunk_link', 'trunk');'''

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
ref_id_col = 'fot_id'

matches = gf.find_matches_buffer(reference_data=reference, osm_data=osm_edges, col_ref_id =ref_id_col, dist=10) # Now we have all osm lines intersecting the reference buffer

#%%
def find_exact_matches(matched_data, osm_data, reference_data, angular_threshold=20, hausdorff_threshold=12, pct_removed_threshold=20, meters_removed_threshold=5):
    
    pass

#%%
osm_matched = []
reference_matched = []
ref_not_matched = []

ref_part_matched = gpd.GeoDataFrame(index = reference.index, columns=[ref_id_col, 'geometry'], crs=crs)
ref_several_matched = pd.DataFrame(index = reference.index, columns=[ref_id_col,'osmid'])
final_matches = gpd.GeoDataFrame(index = reference.index, columns = [ref_id_col,'osmid','osm_index','geometry'], crs=crs)

angular_threshold = 30 # threshold in degress
hausdorff_threshold = 15 # threshold in meters

pct_removed_threshold = 20 # in pct
meters_removed_threshold = 5 # in meters

#%%
def save_best_match(osm_index, ref_index, ref_id_col, row, potential_matches, final_matches, partially_matched, clipped_edges, pct_removed_threshold = pct_removed_threshold, meters_removed_threshold = meters_removed_threshold):

    '''
    Parameters:
        - index of matched osm feature 
        - index of matched reference feature
        - row in reference data that is being matched
        - dataframe with osm matches (potential/best matches)
        - thresholds for pct/meters removed
        - Dataframes used for: 
            - final_matches
            - partially matched
            - osm_matches
            - clipped_edges
    '''

    reference_matched.append(ref_index)
    osm_matched.append(osm_index)

    final_matches.at[ref_index, ref_id_col] = row[ref_id_col]
    final_matches.at[ref_index, 'osmid'] = potential_matches.loc[osm_index, 'osmid']
    final_matches.at[ref_index, 'osm_index'] = osm_index
    final_matches.at[ref_index, 'geometry'] = potential_matches.loc[osm_index, 'geometry']

    if clipped_edges.loc[osm_index, 'pct_removed'] > pct_removed_threshold and clipped_edges.loc[osm_index, 'meters_removed'] > meters_removed_threshold:
    
        #Save removed geometries
        removed_edge = clipped_edges.loc[osm_index, ['geometry']].values[0]
        partially_matched.at[ref_index, 'geometry'] = removed_edge
        partially_matched.at[ref_index, ref_id_col] = row[ref_id_col]

#%%
for ref_index, row in matches.loc[matches.fot_id==1087315069].iterrows(): # TODO: Use something else than iterrows?

    # If no matches exist at all, continue to next reference geometry and add the reference feature as unmatched
    if len(row.matches_index) < 1:

        ref_not_matched.append(row[ref_id_col])

        continue

    else:

        # Get the original geometry for the reference feature
        ref_edge = reference.loc[ref_index].geometry

        
        if ref_edge.geom_type == 'MultiLineString': # Some steps will not work for MultiLineString - convert those to LineString
            ref_edge = linemerge(ref_edge) # This step assumes that MultiLineString do not have gaps!

        # Get the original geometries that intersected this reference geometry's buffer
        osm_df = osm_edges[['osmid','highway','name','geometry']].iloc[row.matches_index].copy(deep=True)

        osm_df['hausdorff_dist'] = None
        osm_df['angle'] = None
        
        #Dataframe for storing how much is removed from the reference geometry with the different OSM matches
        clipped_edges = gpd.GeoDataFrame(index=osm_df.index, columns=['clipped_length','meters_removed','pct_removed',ref_id_col,'geometry'], crs=crs)
        clipped_edges['geometry'] = None

        # Loop through all matches and compute how good of a match they are
        for osm_i, r in osm_df.iterrows():

            osm_edge = r.geometry # Get the geometry of this specific matched OSM edge

            '''
            osm_start_node = Point(osm_edge.coords[0])
            osm_end_node = Point(osm_edge.coords[-1])

            # Get nearest point on reference geometry to start and end nodes of OSM match
            queried_point, nearest_point_start = nearest_points(osm_start_node, ref_edge)
            queried_point, nearest_point_end = nearest_points(osm_end_node, ref_edge)
    
            # Clip reference geometry with nearest points
            clip_points = MultiPoint([nearest_point_start, nearest_point_end])

            clipped_lines_geoms = split(ref_edge, clip_points).geoms
            
            '''
            clip_points, clipped_lines_geoms = split_edge(ref_edge, osm_edge)
           

            # Check if line is clipped at all by checking how many geometries are returned by split function
            if len(clipped_lines_geoms) == 1:

                # Line is not clipped - they are either completely identical OR perpendicular
                # Continue with the unclipped edge
                clipped_ref_edge = clipped_lines_geoms[0]

                print('Hello')

            else: # Line is clipped

                if len(split(ref_edge, clip_points).geoms) == 2: # This happens if one of the clip points are identical to a start or end point on the line
                    clipped_ref_edge = clipped_lines_geoms[0] 

                    print('Line is clipped 1')
                
                elif len(split(ref_edge, clip_points).geoms) == 3: 
                    clipped_ref_edge = clipped_lines_geoms[1] # Keep the clipped geometry placed between the two points

                    print('Line is clipped 2!')

                meters_removed = ref_edge.length - clipped_ref_edge.length
                pct_removed = meters_removed * 100 / ref_edge.length

                clipped_edges.at[osm_i,'clipped_length'] = clipped_ref_edge.length
                clipped_edges.at[osm_i,'meters_removed'] = meters_removed
                clipped_edges.at[osm_i,'pct_removed'] = pct_removed

                clipped_edges.at[osm_i, 'geometry'] = gf.get_geom_diff(ref_edge, clipped_ref_edge)


            # If length of clipped reference edge is very small it indicates that the OSM edge is perpendicular with the reference edge and thus not a correct match
            if clipped_ref_edge.length < 1:
                continue

            # Compute Hausdorff distance (max distance between two lines) # TODO: Convert to function
            #osm_coords = list(osm_edge.coords)
            #ref_coords = list(clipped_ref_edge.coords)
            #hausdorff_dist = max(directed_hausdorff(osm_coords, ref_coords)[0], directed_hausdorff(ref_coords, osm_coords)[0])
            
            hausdorff_dist = gf.get_hausdorff_dist(osm_edge=osm_edge, ref_edge=clipped_ref_edge)
            osm_df.at[osm_i, 'hausdorff_dist'] = hausdorff_dist

            # Angular difference
            angle_deg = gf.get_angle(osm_edge, ref_edge)
            osm_df.at[osm_i, 'angle'] = angle_deg

        # Find potential matches out of all matches for this referehce geometry - osm_i.e. filter out matches that are not within thresholds
        potential_matches = osm_df[ (osm_df.angle < angular_threshold) & (osm_df.hausdorff_dist < hausdorff_threshold)]

        if len(potential_matches) == 0:

            ref_not_matched.append(row[ref_id_col])

            continue
        
        elif len(potential_matches) == 1:

            osm_ix = potential_matches.index.values[0]

            save_best_match(osm_index=osm_ix, ref_index=ref_index, ref_id_col=ref_id_col, row=row, potential_matches=osm_df,final_matches=final_matches, partially_matched=ref_part_matched, clipped_edges=clipped_edges)

            '''
            reference_matched.append(index)
            osm_matched.append(potential_matches.index.values[0])

            final_matches.at[index, ref_id_col] = row[ref_id_col]
            final_matches.at[index, 'osmid'] = potential_matches.osmid.values[0]
            final_matches.at[index, 'geometry'] = potential_matches.geometry.values[0]

            # First check if osmid have been added to clipped edges df at all - only happens if the reference edge is actually clipped
            if clipped_edges.loc[potential_matches.index[0], 'pct_removed'] > pct_removed_threshold and clipped_edges.loc[potential_matches.index[0], 'meters_removed'] > meters_removed_threshold:
                
                #Save removed geometries
                removed_edge = clipped_edges.loc[potential_matches.index[0], ['geometry']].values[0]
                ref_part_matched.at['geometry'] = removed_edge
            
            '''
        
        else:

            # Get match(es) with smallest Hausdorff distance and angular tolerance
            osm_df['hausdorff_dist'] = pd.to_numeric(osm_df['hausdorff_dist'] )
            osm_df['angle'] = pd.to_numeric(osm_df['angle'])
            
            #best_matches = osm_df[['hausdorff_dist','angle']].min()
            best_matches_index = osm_df[['hausdorff_dist','angle']].idxmin()
            best_matches = osm_df.loc[best_matches_index].copy(deep=True)
            
            best_matches = best_matches[~best_matches.index.duplicated(keep='first')]

            if len(best_matches) == 1:

                osm_ix = best_matches.index.values[0]

                save_best_match(osm_index=osm_ix, ref_index=ref_index, ref_id_col=ref_id_col, row=row, potential_matches=osm_df,final_matches=final_matches, partially_matched=ref_part_matched, clipped_edges=clipped_edges)

        
                '''
                reference_matched.append(index)
                osm_matched.append(best_matches.index.values[0])

                final_matches.at[index, ref_id_col] = row[ref_id_col]
                final_matches.at[index, 'osmid'] = best_matches.osmid.values[0]
                final_matches.at[index, 'geometry'] = best_matches.geometry.values[0]

                if clipped_edges.loc[best_matches.index[0], 'pct_removed'] > pct_removed_threshold and clipped_edges.loc[best_matches.index[0], 'meters_removed'] > meters_removed_threshold:
                
                    #Save removed geometries
                    removed_edge = clipped_edges.loc[best_matches.index[0], ['geometry']].values[0]
                    ref_part_matched.at['geometry'] = removed_edge
                
                '''

            elif len(best_matches) > 1: # Take the one with the smallest hausdorff distance
                
                #ref_several_matched.at[ref_index, ref_id_col] = row[ref_id_col]
                #ref_several_matched.at[ref_index, 'osmid'] = list(best_matches.osmid)
                
                best_match_index = best_matches['hausdorff_dist'].idxmin()
                best_match = osm_df.loc[best_match_index].copy(deep=True)
                best_match = best_match[~best_match.index.duplicated(keep='first')]
        
                # Save result
            
                osm_ix = best_match.name

                save_best_match(osm_index=osm_ix, ref_index=ref_index, ref_id_col=ref_id_col, row=row, potential_matches=osm_df,final_matches=final_matches, partially_matched=ref_part_matched, clipped_edges=clipped_edges)

#%%
1214470881
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

not_matched = reference.loc[reference.fot_id.isin(ref_not_matched)]

dbf.to_postgis(geodataframe=not_matched, table_name='not_matched', engine=engine)

connection.close()

# %%
reference.loc[reference.fot_id.isin(ref_not_matched)].to_file('../data/not_matched.gpkg', driver='GPKG')

#%%
for index, row in matches.iterrows():
    mi = row.matches_index
    if len(mi) == 0:
        matches.at[index, 'matches_index'] = 0
        matches.at[index, 'matches_osmid'] = 0
# %%

def split_edge(line_to_split, split_line):

    '''
    Function for clipping one LineString to the extent of another LineString
    '''

    start_node = Point(split_line.coords[0])
    end_node = Point(split_line.coords[-1])

    # Get nearest point on reference geometry to start and end nodes of OSM match
    org_point, nearest_point_start = nearest_points(start_node, line_to_split)
    org_point2, nearest_point_end = nearest_points(end_node, line_to_split)

    print(nearest_point_start)
    print(nearest_point_end)
   
    if nearest_point_start.within(line_to_split) == False or nearest_point_end.within(line_to_split) == False:
        
        new_nearest_start = snap(nearest_point_start,line_to_split, 0.01)
        new_nearest_end= snap(nearest_point_end,line_to_split, 0.01)

        assert new_nearest_start.within(line_to_split)
        assert new_nearest_end.within(line_to_split)

        clip_points = MultiPoint([new_nearest_start, new_nearest_end])

        clipped_lines_geoms = split(line_to_split, clip_points)
    
    else:
        # Clip geometry with nearest points
        clip_points = MultiPoint([nearest_point_start, nearest_point_end])

        clipped_lines_geoms = split(line_to_split, clip_points)

    return clipped_lines_geoms

# Problem - it works if the end point is moved but not if the nearest point is moved
# Possibly because of differing accuraries??
# I could just take the new line returned by both and then use their start/end points to clip??
#%%
def split_edge_buffer(line_to_split, split_line):

    '''
    Function for clipping one LineString to the extent of another LineString
    '''

    start_node = Point(split_line.coords[0])
    end_node = Point(split_line.coords[-1])

    # Get nearest point on reference geometry to start and end nodes of OSM match
    org_point, nearest_point_start = nearest_points(start_node, line_to_split)
    org_point2, nearest_point_end = nearest_points(end_node, line_to_split)

   
    if nearest_point_start.within(line_to_split) == False or nearest_point_end.within(line_to_split) == False:
        new_line = LineString([nearest_point_start, nearest_point_end])
        buff = new_line.buffer(0.00001)

        #first_seg, buff_seg, last_seg 
        clipped_lines_geoms = split(line_to_split,buff)

    else:

        # Clip geometry with nearest points
        clip_points = MultiPoint([nearest_point_start, nearest_point_end])

        clipped_lines_geoms = split(line_to_split, clip_points)

    return clipped_lines_geoms, buff

#%%
#############################################
geodk_line = LineString( [(1,1),(10,10)])

osm_line = LineString([(1,2),(7,9)])

#%%

p, l = split_edge(line_to_split = geodk_line, split_line=osm_line)

#%%

osm_edge = LineString( [ (719828.246076221,6177474.258233718), (719868.4017477125,6177465.392409511) ])
ref_edge = LineString( [ (719797.25,6177490.79), (719805.72,6177488.3), (719810.68,6177487.09), (719868.52,6177472.95) ])

#%%
test, buff_test = split_edge_buffer(line_to_split=ref_edge, split_line=osm_edge)

#%%

start_node = Point(osm_edge.coords[0])
end_node = Point(osm_edge.coords[-1])

# Get nearest point on reference geometry to start and end nodes of OSM match
org_point, nearest_point_start = nearest_points(start_node, ref_edge)
org_point2, nearest_point_end = nearest_points(end_node, ref_edge)

print(nearest_point_start)
print(nearest_point_end)

nearest_buff = nearest_point_end.buffer(0.001)

print(nearest_buff.intersects(ref_edge))

test = snap(nearest_point_end, ref_edge, 0.01)

#%%

#points_gdf = gpd.GeoDataFrame(index=osm_df.index, columns=['geometry','index'], crs=crs)

lines_list = []

for index, row in osm_df.iterrows():

    lines = split_edge(line_to_split=ref_edge, split_line=row.geometry)

    print(len(lines.geoms))

    #print(buff.area)

    print(lines.geoms[0].length)

    #points_gdf.at[index, 'geometry'] = points

    lines_list.append(lines.geoms[1])
 
#%%
lines_gdf = gpd.GeoDataFrame(index=osm_df.index, geometry=lines_list, crs=crs)

#points_gdf['index'] = points_gdf.index
lines_gdf['index'] = lines_gdf.index

#%%
engine = dbf.connect_alc(db_name, db_user, db_password, db_port=db_port)

#dbf.to_postgis(geodataframe=points_gdf, table_name='points', engine=engine)

dbf.to_postgis(geodataframe=lines_gdf, table_name='lines2', engine=engine)

# %%

#%%
