'''
Script for matching road networks.
Method assumes a simplified version of public dataset and a non-simplified version of OSM (osm_i.e. that edges in the reference_data dataset usually are as long or longer than OSM).
Use case is situations where a specific attribute is poorly defined in OSM, and the script is thus developed for a situation where it is assumed that most core road geometries are present in both data sets.
But it is also possible to use for identifying edges in the reference_data dataset with no match in OSM or vice-versa.

'''
# TODO: Docs

# TODO: Functionality for doing analysis grid by grid

# TODO: Function for adding unmatched to dataset?

# Problem with curved lines
# Problem when OSM is much longer? - Use simplified networks!

#%%
import geopandas as gpd
import pandas as pd
import yaml
from src import db_functions as dbf
from src import geometric_functions as gf
from shapely.ops import linemerge
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

reference_data = gpd.GeoDataFrame.from_postgis(get_geodk, connection, geom_col='geometry' )

grid = gpd.GeoDataFrame.from_postgis(get_grid, connection, geom_col='geometry')

assert osm_edges.crs == crs
assert reference_data.crs == crs
assert grid.crs == crs

print(f'Number of rows in osm_edge table: {len(osm_edges)}')
print(f'Number of rows in reference_data table: {len(reference_data)}')
print(f'Number of rows in grid table: {len(grid)}')

#%%
'''
# For analysing larger areas - do a grid by grid matching
env = gpd.GeoDataFrame()
env.at[0,'geometry'] = reference_data.unary_union.envelope
env = env.set_crs(crs) 

clipped_grid = gpd.clip(grid, env)
'''

#%%
# Define name of id col in ref dataset
ref_id_col = 'fot_id'

#%%
# Find matches based on buffer distance
matches = gf.find_matches_buffer(reference_data=reference_data, osm_data=osm_edges, col_ref_id =ref_id_col, dist=12) 

#%%
def find_exact_matches(matches, osm_edges, reference_data, ref_id_col, angular_threshold=20, hausdorff_threshold=15, pct_removed_threshold=20, meters_removed_threshold=5):

    ref_part_matched = gpd.GeoDataFrame(index = reference_data.index, columns=[ref_id_col, 'meters_removed','pct_removed','geometry'], crs=crs)
    final_matches = gpd.GeoDataFrame(index = reference_data.index, columns = [ref_id_col,'osmid','osm_index','geometry'], crs=crs)

    angular_threshold = 30 # threshold in degress
    hausdorff_threshold = 15 # threshold in meters

    pct_removed_threshold = 20 # in pct
    meters_removed_threshold = 5 # in meters

    
    for ref_index, row in matches.iterrows(): # TODO: Use something else than iterrows for better performance?

        # If no matches exist at all, continue to next reference_data geometry and add the reference_data feature as unmatched
        if len(row.matches_index) < 1:

            #ref_not_matched.append(row[ref_id_col])

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

                clipped_ref_edge = gf.clip_new_edge(line_to_split=ref_edge, split_line=osm_edge)
            
                meters_removed = ref_edge.length - clipped_ref_edge.length
                pct_removed = meters_removed * 100 / ref_edge.length

                clipped_edges.at[osm_i,'clipped_length'] = clipped_ref_edge.length
                clipped_edges.at[osm_i,'meters_removed'] = meters_removed
                clipped_edges.at[osm_i,'pct_removed'] = pct_removed
                clipped_edges.at[osm_i, ref_id_col] = row[ref_id_col]
                clipped_edges.at[osm_i, 'geometry'] = gf.get_geom_diff(ref_edge, clipped_ref_edge.buffer(0.001))

                # If length of clipped reference_data edge is very small it indicates that the OSM edge is perpendicular with the reference_data edge and thus not a correct match
                if clipped_ref_edge.length < 1:

                    clipped_edges.drop(labels=osm_i, inplace=True)
                    
                    continue

                hausdorff_dist = gf.get_hausdorff_dist(osm_edge=osm_edge, ref_edge=clipped_ref_edge)
                osm_df.at[osm_i, 'hausdorff_dist'] = hausdorff_dist

                angle_deg = gf.get_angle(osm_edge, ref_edge)
                osm_df.at[osm_i, 'angle'] = angle_deg

            # Find matches within thresholds out of all matches for this referehce geometry
            potential_matches = osm_df[ (osm_df.angle < angular_threshold) & (osm_df.hausdorff_dist < hausdorff_threshold)]

            if len(potential_matches) == 0:

                #ref_not_matched.append(row[ref_id_col])

                continue
            
            elif len(potential_matches) == 1:

                osm_ix = potential_matches.index.values[0]

                gf.save_best_match(osm_index=osm_ix, ref_index=ref_index, ref_id_col=ref_id_col, row=row, potential_matches=osm_df,final_matches=final_matches, partially_matched=ref_part_matched, clipped_edges=clipped_edges, pct_removed_threshold=pct_removed_threshold, meters_removed_threshold=meters_removed_threshold)
            
            else:

                # Get match(es) with smallest Hausdorff distance and angular tolerance
                osm_df['hausdorff_dist'] = pd.to_numeric(osm_df['hausdorff_dist'] )
                osm_df['angle'] = pd.to_numeric(osm_df['angle'])
                
                best_matches_index = osm_df[['hausdorff_dist','angle']].idxmin()
                best_matches = osm_df.loc[best_matches_index].copy(deep=True)
                
                best_matches = best_matches[~best_matches.index.duplicated(keep='first')] # Duplicates may appear if the same edge is the one with min dist and min angle

                if len(best_matches) == 1:

                    osm_ix = best_matches.index.values[0]

                    gf.save_best_match(osm_index=osm_ix, ref_index=ref_index, ref_id_col=ref_id_col, row=row, potential_matches=osm_df,final_matches=final_matches, partially_matched=ref_part_matched, clipped_edges=clipped_edges, pct_removed_threshold=pct_removed_threshold, meters_removed_threshold=meters_removed_threshold)

                elif len(best_matches) > 1: # Take the one with the smallest hausdorff distance
                    
                    best_match_index = best_matches['hausdorff_dist'].idxmin()
                    best_match = osm_df.loc[best_match_index].copy(deep=True)
                    best_match = best_match[~best_match.index.duplicated(keep='first')]
            
                    osm_ix = best_match.name # Save result

                    gf.save_best_match(osm_index=osm_ix, ref_index=ref_index, ref_id_col=ref_id_col, row=row, potential_matches=osm_df,final_matches=final_matches, partially_matched=ref_part_matched, clipped_edges=clipped_edges, pct_removed_threshold=pct_removed_threshold, meters_removed_threshold=meters_removed_threshold)
    
    final_matches.dropna(inplace=True)
    ref_part_matched.dropna(inplace=True)

    print(f'{len(final_matches)} reference edges where matched to OSM edges')
    print(f'Out of those, {len(ref_part_matched)} reference edges where only partially matched to OSM edges')
    print(f'{ len(reference_data) - len(final_matches) } reference edges where not matched')
    
    return final_matches, ref_part_matched


#%%
# Find exact matches 
final_matches, partially_matched = gf.find_exact_matches(matches=matches, osm_edges=osm_edges, reference_data=reference_data, 
ref_id_col=ref_id_col, crs=crs)

#%%

# TODO: Rerun for partial matches
# First, remove already matched OSM edges from analysis
# Split multilinestrings into two rows
# For how long? Look at results from each run - are they valid?
# Repeat buffer operation for this step


# TODO: Quality check (optional)
# If you have data on correct matches - check result against this and compute score


# TODO: Add new info to OSM data
# Compute how many have been updated (i.e. where not in OSM bike before)


# TODO: Add unmatched to dataset
# Not just a question of adding to database - should create uniform col names, geometric structure (i.e. simplified or not)

#######################################################
#%%
osm_matched = []
reference_matched = []
ref_not_matched = []

ref_part_matched = gpd.GeoDataFrame(index = reference_data.index, columns=[ref_id_col, 'geometry'], crs=crs)
ref_several_matched = pd.DataFrame(index = reference_data.index, columns=[ref_id_col,'osmid'])
final_matches = gpd.GeoDataFrame(index = reference_data.index, columns = [ref_id_col,'osmid','osm_index','geometry'], crs=crs)

angular_threshold = 30 # threshold in degress
hausdorff_threshold = 15 # threshold in meters

pct_removed_threshold = 20 # in pct
meters_removed_threshold = 5 # in meters

#%%
for ref_index, row in matches.iterrows(): # TODO: Use something else than iterrows for better performance?

    # If no matches exist at all, continue to next reference_data geometry and add the reference_data feature as unmatched
    if len(row.matches_index) < 1:

        ref_not_matched.append(row[ref_id_col])

        continue

    else:

        # Get the original geometry for the reference_data feature
        ref_edge = reference_data.loc[ref_index].geometry
        
        if ref_edge.geom_type == 'MultiLineString': # Some steps will not work for MultiLineString - convert those to LineString
            ref_edge = linemerge(ref_edge) # This step assumes that MultiLineString do not have gaps!

        # Get the original geometries that intersected this reference_data geometry's buffer
        osm_df = osm_edges[['osmid','highway','name','geometry']].iloc[row.matches_index].copy(deep=True)

        osm_df['hausdorff_dist'] = None
        osm_df['angle'] = None
        
        #Dataframe for storing how much is removed from the reference_data geometry with the different OSM matches
        clipped_edges = gpd.GeoDataFrame(index=osm_df.index, columns=['clipped_length','meters_removed','pct_removed',ref_id_col,'geometry'], crs=crs)
        clipped_edges['geometry'] = None

        # Loop through all matches and compute how good of a match they are
        for osm_i, r in osm_df.iterrows():

            osm_edge = r.geometry # Get the geometry of this specific matched OSM edge

            clipped_ref_edge = gf.clip_new_edge(line_to_split=ref_edge, split_line=osm_edge)
        
            meters_removed = ref_edge.length - clipped_ref_edge.length
            pct_removed = meters_removed * 100 / ref_edge.length

            clipped_edges.at[osm_i,'clipped_length'] = clipped_ref_edge.length
            clipped_edges.at[osm_i,'meters_removed'] = meters_removed
            clipped_edges.at[osm_i,'pct_removed'] = pct_removed

            #clipped_edges.at[osm_i, 'geometry'] = gf.get_geom_diff(ref_edge, clipped_ref_edge)

            clipped_edges.at[osm_i, 'geometry'] = gf.get_geom_diff(ref_edge, clipped_ref_edge.buffer(0.001))

            # If length of clipped reference_data edge is very small it indicates that the OSM edge is perpendicular with the reference_data edge and thus not a correct match
            if clipped_ref_edge.length < 1:
                continue

            # Compute Hausdorff distance (max distance between two lines) # TODO: Convert to function
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

            gf.save_best_match(osm_index=osm_ix, ref_index=ref_index, ref_id_col=ref_id_col, row=row, potential_matches=osm_df,final_matches=final_matches, partially_matched=ref_part_matched, clipped_edges=clipped_edges, pct_removed_threshold=pct_removed_threshold, meters_removed_threshold=meters_removed_threshold)
        
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

                gf.save_best_match(osm_index=osm_ix, ref_index=ref_index, ref_id_col=ref_id_col, row=row, potential_matches=osm_df,final_matches=final_matches, partially_matched=ref_part_matched, clipped_edges=clipped_edges, pct_removed_threshold=pct_removed_threshold, meters_removed_threshold=meters_removed_threshold)

            elif len(best_matches) > 1: # Take the one with the smallest hausdorff distance
                
                #ref_several_matched.at[ref_index, ref_id_col] = row[ref_id_col]
                #ref_several_matched.at[ref_index, 'osmid'] = list(best_matches.osmid)
                
                best_match_index = best_matches['hausdorff_dist'].idxmin()
                best_match = osm_df.loc[best_match_index].copy(deep=True)
                best_match = best_match[~best_match.index.duplicated(keep='first')]
        
                # Save result
                osm_ix = best_match.name

                gf.save_best_match(osm_index=osm_ix, ref_index=ref_index, ref_id_col=ref_id_col, row=row, potential_matches=osm_df,final_matches=final_matches, partially_matched=ref_part_matched, clipped_edges=clipped_edges, pct_removed_threshold=pct_removed_threshold, meters_removed_threshold=meters_removed_threshold)

#%%
# Upload result to DB
engine = dbf.connect_alc(db_name, db_user, db_password, db_port=db_port)

not_matched = reference_data.loc[reference_data.fot_id.isin(ref_not_matched)]

dbf.to_postgis(geodataframe=not_matched, table_name='not_matched', engine=engine)

connection.close()

# %%
reference_data.loc[reference_data.fot_id.isin(ref_not_matched)].to_file('../data/not_matched.gpkg', driver='GPKG')

#%%
for index, row in matches.iterrows():
    mi = row.matches_index
    if len(mi) == 0:
        matches.at[index, 'matches_index'] = 0
        matches.at[index, 'matches_osmid'] = 0
# %%

#%%
#############################################


#%%

#points_gdf = gpd.GeoDataFrame(index=osm_df.index, columns=['geometry','index'], crs=crs)

lines_list = []

for index, row in osm_df.iterrows():

    lines = gf.split_edge_buffer(line_to_split=ref_edge, split_line=row.geometry)

    print('\n')
    print(index)
    print(len(lines.geoms))

    #print(buff.area)
    print('Length of OSM edge:', row.geometry.length)
    #print(lines.geoms[0].length)
    for l in lines.geoms:
        print('Clipped lengths:', l.length)

    #points_gdf.at[index, 'geometry'] = points

    lines_list.append(lines.geoms[1])
 
#%%
lines_gdf = gpd.GeoDataFrame(index=osm_df.index, geometry=lines_list, crs=crs)

#points_gdf['index'] = points_gdf.index
lines_gdf['index'] = lines_gdf.index

# %%
