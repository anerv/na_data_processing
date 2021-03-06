def update_osm_old(matches, osm_data, ref_data, ref_col, new_col, compare_col= None):

    '''
    Function for updating OSM based on matches with reference dataset. 
    Current version only accepts one column for reference data and one column in OSM data.

    Parameters
    ----------
    matched_data: GeoDataFrame
        Index and ids of matches

    osm_data: GeoDataFrame
        OSM edges to be updated

    ref_data: GeoDataFrame
        Reference data with attributes to be transfered to OSM data

    Returns
    -------
    
    '''

    osm_data[new_col] = None

    ref_matches = ref_data[ref_col].loc[matches.index].values

    osm_data.loc[matches.osm_index, new_col] = ref_matches

    count_updates = len(osm_data.loc[matches.osm_index])

    print(f'{count_updates} OSM edges were updated!')

    if compare_col:
        diff = count_updates - np.count_nonzero(osm_data[compare_col])
        print(f'{diff} OSM edges did not already have this information!')

    # Check for conflicting updates
    count_conflicts = 0

    duplicates = matches.osm_index.value_counts()
    duplicates = duplicates[duplicates > 1]
    dup_index = duplicates.index.to_list()

    for i in dup_index:

        same_matches = matches.loc[matches.osm_index == i].index
        ref_values = ref_data.loc[same_matches, ref_col]
        
        if len(ref_values.unique()) > 1:
         
            count_conflicts += 1
        
    print(f'{count_conflicts} OSM edges had conflicting matches!')

    return osm_data


def create_segment_gdf_old(gdf, segment_length, id_col):

    # New geodataframe for storing segments
    segments_gdf = gpd.GeoDataFrame()

    for _, row in gdf.iterrows():

        org_id = row[id_col]

        if row.geometry.geom_type == 'MultiLineString':
            org_geom = linemerge(row.geometry)
        
        else:
            org_geom = row.geometry

        new_geoms = get_segments(org_geom, segment_length)

        new_gdf = gpd.GeoDataFrame(geometry=new_geoms)
        new_gdf[id_col] = org_id

        segments_gdf = pd.concat([segments_gdf, new_gdf], ignore_index=True)

        # When all features have been cut into segments, add unique id
        ids = []
        for i in range(1000, 1000+len(segments_gdf)):
            ids.append(i)

        segments_gdf['seg_id'] = ids

        assert len(segments_gdf['seg_id'].unique()) == len(segments_gdf)


    return segments_gdf



def get_segments(linestring, seg_length):

    org_length = linestring.length

    no_segments = math.ceil(org_length / seg_length)

    #no_segments = round(org_length / seg_length)

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
    for i, l in enumerate(lines):
        if l.length < seg_length/3:
            new_l = linemerge((lines[i-1], l))

            lines[i-1] = new_l

            del lines[i]

    return lines


def find_matches_buffer(reference_data, osm_data, ref_id_col, dist):

    '''
    Function for finding which OSM edges intersect a buffered reference data set.
    The first step in a matching of OSM with another line data set.

    Parameters
    ----------
    reference_data: GeoDataFrame (geopandas)
        GDF with edges (LineStrings) to be matched to OSM edges.

    osm_data: GeoDataFrame (geopandas)
        GeoDataFrame with OSM edges which the reference data should be matched to.

    ref_id_col: String
        Name of column with unique ID for reference feature

    dist: Numerical
        Max distance between distances that should be considered a match (used for creating the buffers)


    Returns
    -------
    matches DataFrame (pandas):
        DataFrame with the reference index as index, a column with reference data unique ID and the index and ID of intersecting OSM edges.

    '''

    # Functionality for accepting a series/row instead of a dataframe
    if type(reference_data) == pd.core.series.Series:
        reference_data = gpd.GeoDataFrame({ref_id_col:reference_data[ref_id_col], 'geometry':reference_data.geometry}, index=[0])

    reference_buff = reference_data.copy(deep=True)
    reference_buff.geometry = reference_buff.geometry.buffer(distance=dist)

    # Create spatial index on osm data
    osm_sindex = osm_data.sindex

    # Create dataframe to store matches in
    matches = pd.DataFrame(index=reference_buff.index, columns=['matches_index','matches_osmid', ref_id_col])

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

        matches.at[index, ref_id_col] = row[ref_id_col]

    return matches


# Not used right now
def find_matches_segments(osm_edges, reference_data, ref_id_col, buffer_dist=10, angular_threshold=30, hausdorff_threshold=12, crs='EPSG:25832'):

    final_matches = pd.DataFrame(columns = [ref_id_col,'osmid','osm_index'])

    assert osm_edges.crs == reference_data.crs, 'Data not in the same crs!'

    for _, row in reference_data.iterrows():

        ref_id = row[ref_id_col]
       
        # Find matches within buffer distance
        buffer_matches = find_matches_buffer(reference_data=row, osm_data=osm_edges, ref_id_col=ref_id_col, dist=buffer_dist)

         # If no matches exist at all, continue to next reference_data geometry and add the reference_data feature as unmatched
        if len(buffer_matches.loc[0,'matches_index']) < 1:

            print('No matches found with buffer!')

            continue

        else:
            ref_edge = row.geometry

            if ref_edge.geom_type == 'MultiLineString':
                # Some steps will not work for MultiLineString - convert those to LineString
                ref_edge = linemerge(ref_edge) # This step assumes that MultiLineString do not have gaps!

            # Get the original geometries that intersected this reference_data geometry's buffer
            potential_matches = osm_edges[['osmid','geometry']].loc[buffer_matches.loc[0,'matches_index']].copy(deep=True)

            potential_matches['hausdorff_dist'] = None
            potential_matches['angle'] = None

            best_osm_ix = find_best_match_segment(potential_matches=potential_matches, reference_edge=ref_edge, angular_threshold=angular_threshold, hausdorff_threshold=hausdorff_threshold)

            if best_osm_ix is None:
                print('No match found out of potential matches!')
                continue

            # Save best match
            save_best_match(final_matches=final_matches, ref_id_col=ref_id_col, ref_id=ref_id, osm_index=best_osm_ix, potential_matches=potential_matches)

    print(f'{len(final_matches)} reference segments where matched to OSM edges')

    print(f'{ len(reference_data) - len(final_matches) } reference segments where not matched')
    
    return final_matches



def find_matches_from_buffer(buffer_matches, osm_edges, reference_data, ref_id_col, angular_threshold=30, hausdorff_threshold=12):

    final_matches = pd.DataFrame(columns = [ref_id_col,'osmid','osm_index'])

    assert osm_edges.crs == reference_data.crs, 'Data not in the same crs!'

    for ref_index, row in reference_data.iterrows():

        ref_id = row[ref_id_col]
    
        # If no matches exist at all, continue to next reference_data geometry and add the reference_data feature as unmatched
        if len(buffer_matches.loc[ref_index,'matches_index']) < 1:

            print('No matches found with buffer!')

            continue

        else:
            ref_edge = row.geometry

            if ref_edge.geom_type == 'MultiLineString':
                # Some steps will not work for MultiLineString - convert those to LineString
                ref_edge = linemerge(ref_edge) # This step assumes that MultiLineString do not have gaps!

            # Get the original geometries that intersected this reference_data geometry's buffer
            potential_matches = osm_edges[['osmid','geometry']].loc[buffer_matches.loc[ref_index,'matches_index']].copy(deep=True)

            potential_matches['hausdorff_dist'] = None
            potential_matches['angle'] = None

            best_osm_ix = find_best_match_segment(potential_matches=potential_matches, reference_edge=ref_edge, angular_threshold=angular_threshold, hausdorff_threshold=hausdorff_threshold)

            if best_osm_ix is None:
                print('No match found out of potential matches!')
                continue

            # Save best match
            save_best_match(final_matches=final_matches, ref_id_col=ref_id_col, ref_id=ref_id, osm_index=best_osm_ix, potential_matches=potential_matches)

    print(f'{len(final_matches)} reference segments where matched to OSM edges')

    print(f'{ len(reference_data) - len(final_matches) } reference segments where not matched')
    
    return final_matches

def save_best_match(final_matches, ref_id_col, ref_id, osm_index, osm_id):

    '''
    Function for saving the best of the potential matches.
    To be used internally in the function for finding the exact match between features in reference and OSM data.

    Parameters
    ----------
    osm_index: index key (string or int)
        Index of matched OSM feature

    ref_id_col: string
        Name of column with unique ID for reference data

    ref_id: string/numeric
        Unique ID of reference edge

    potential_matches: 
        Dataframe with OSM edges that are potential matches

    final_matches: pandas DataFrame
        DataFrame used to store final mathces


    Returns
    -------
    None:
        Updates dataframe with final matches
    '''

    if final_matches.last_valid_index() == None:
        new_ix = 1
    else:
        new_ix = final_matches.last_valid_index() + 1

    final_matches.at[new_ix, ref_id_col] = ref_id
    final_matches.at[new_ix, 'osmid'] = osm_id
    final_matches.at[new_ix, 'osm_index'] = osm_index


# Test for saving matches function
    ref = gpd.read_file('../tests/geodk_small_test.gpkg')
    osm = gpd.read_file('../tests/osm_small_test.gpkg')

    ref_segments = create_segment_gdf(ref, segment_length=5)
    osm_segments = create_segment_gdf(osm, segment_length=5)

    osm_segments['old_osmid'] = osm_segments.osmid
    osm_segments.osmid = osm_segments.seg_id

    osm_segments.set_crs('EPSG:25832', inplace=True)
    ref_segments.set_crs('EPSG:25832', inplace=True)

    buffer_matches = return_buffer_matches(osm_data=osm_segments, reference_data=ref_segments, ref_id_col='seg_id', dist=10)

    final_matches = pd.DataFrame(columns = ['seg_id','osmid','osm_index'])

    test_values_ix = {
        13: 114,
        14: 115,
        15: 72,
        44: 133,
        12: 113,
        22: 113,
        23: 112}

    for key, value in test_values_ix.items():

        potential_matches_test = osm_segments[['osmid','geometry']].loc[buffer_matches.loc[key,'matches_index']].copy(deep=True)
        ref_edge = ref_segments.loc[key,'geometry']
        test_match = find_best_match(potential_matches_test, reference_edge=ref_edge, hausdorff_threshold=12, angular_threshold=20)
        
        save_best_match(final_matches=final_matches, ref_id_col='seg_id', ref_id=ref_segments.loc[key,'seg_id'], osm_index=test_match, potential_matches=potential_matches_test)

    
    test_values_id = {
        1013: 1114,
        1014: 1115,
        1015: 1072,
        1044: 1133,
        1012: 1113,
        1022: 1113,
        1023: 1112}

    assert len(final_matches == len(test_values_id))

    for key, value in test_values_id.items():
        osm_ix = final_matches['osmid'].loc[final_matches.seg_id==key].values[0]
        assert osm_ix == value

def overlay_buffer(osm_data, reference_data, dist, ref_id_col):

    assert osm_data.crs == reference_data.crs, 'Data not in the same crs!'

    reference_buff = reference_data[[ref_id_col, 'geometry']].copy(deep=True)
    reference_buff.geometry = reference_buff.geometry.buffer(distance=dist)

    # Overlay buffered geometries and osm segments
    joined = gpd.overlay(reference_buff, osm_data, how='intersection', keep_geom_type=False)
    
    # Group by id - find all matches for each ref segment
    grouped = joined.groupby(ref_id_col)

    reference_buff['matches_id'] = None

    for group_id, group in grouped:

        matches_ids = group.osmid.to_list()

        ix = reference_buff.loc[reference_buff[ref_id_col] == group_id].index

        reference_buff.at[ix[0], 'matches_id'] = matches_ids

    # Count matches
    reference_buff['count'] = reference_buff['matches_id'].apply(lambda x: len(x) if type(x) == list else 0)

    reference_buff = reference_buff[reference_buff['count'] >= 1]

    reference_buff.drop('geometry', axis=1, inplace=True)

    return reference_buff



def find_matches_buffer(geom, spatial_index, osm_data):

    '''
    Arguments:
        geom (Shapely polygon): Geometry used to find intersecting OSM data
        spatial_index: Spatial index for the OSM data
        osm_data (geodataframe): Data to be matched to the geometry

    Returns:
        precise_matches_index (list): A list with the indices of OSM features which intersects the buffer geom.
    '''

    possible_matches_index = list(spatial_index.intersection(geom.bounds))
    possible_matches = osm_data.iloc[possible_matches_index]

    precise_matches = possible_matches[possible_matches.intersects(geom)]
    precise_matches_index = list(precise_matches.index)

    return precise_matches_index

##############################

def return_buffer_matches(reference_data, osm_data, ref_id_col, dist):

    '''
    Function which for each feature/geometry in the reference_data buffers the geometry,
    and the finds features in the osm_data that intersects the buffer. 

    Arguments:
        reference_data (geodataframe): Data to buffer and find intersecting features in osm_data
        osm_data (geodataframe): Data to test for intersection with reference_data buffers
        ref_id_col (str): Name of column with unique id for reference features
        dist (numeric): How much the geometries should be buffered (in meters)

    Returns:
        reference_buff (dataframe): A dataframe with the original index and ids of the reference data and a new column with lists of indices of intersecting osm features.
    
    '''

    assert osm_data.crs == reference_data.crs, 'Data not in the same crs!'

    reference_buff = reference_data[[ref_id_col, 'geometry']].copy(deep=True)
    reference_buff.geometry = reference_buff.geometry.buffer(distance=dist)

    # Create spatial index on osm data
    osm_sindex = osm_data.sindex

    reference_buff['matches_index'] = reference_buff['geometry'].apply(lambda x: find_matches_buffer(x, osm_sindex, osm_data))

    # Drop geometry column
    reference_buff.drop('geometry', axis=1, inplace=True)

    # Only return rows with a result
    reference_buff['count'] = reference_buff['matches_index'].apply(lambda x: len(x))
    reference_buff = reference_buff[reference_buff['count'] > 1]
  
    return reference_buff


# Test buffer matches function
ref = gpd.read_file('../tests/geodk_small_test.gpkg')
osm = gpd.read_file('../tests/osm_small_test.gpkg')

fot_id = 1095203923
index = ref.loc[ref.fot_id==fot_id].index.values[0]
correct_osm_matches_id = [17463, 17466, 17467, 17472, 17473, 58393, 58394]
correct_osm_matches_ix = osm.loc[osm.osmid.isin(correct_osm_matches_id)].index.to_list()

buffer_matches = overlay_buffer(ref, osm, 'fot_id', 10)

assert ['fot_id','matches_index'] == buffer_matches.columns.to_list()

assert type(buffer_matches) == gpd.geodataframe.GeoDataFrame

if len(buffer_matches) > 1:
    for b in buffer_matches['matches_index'].loc[index]:
        assert b in correct_osm_matches_ix

    assert len(correct_osm_matches_ix) == len(buffer_matches['matches_index'].loc[index])

else:
    for b in buffer_matches['matches_index'].loc[0]:
        assert b in correct_osm_matches_ix

    assert len(correct_osm_matches_ix) == len(buffer_matches['matches_index'].loc[0])


def find_matches_from_buffer(buffer_matches, osm_edges, reference_data, angular_threshold=30, hausdorff_threshold=12):
    '''
    Finds the best/correct matches in two datasets with linestrings, from an initial matching based on a buffered intersection.

    Arguments:
        buffer_matches (dataframe): Outcome of buffer intersection step
        reference_data (geodataframe): reference data to be matched to osm data
        osm_edges (geodataframe): osm data to be matched to reference data
        angular_threshold (numerical): Threshold for max angle between lines considered a match (in degrees)
        hausdorff_threshold: Threshold for max Hausdorff distance between lines considered a match (in meters)

    Returns:
        matched_data (geodataframe): Reference data with additional columns specifying the index and ids of matched osm edges
    '''

    # Get edges matched with buffer
    matched_data = reference_data.loc[buffer_matches.index].copy(deep=True)

    # Find best match within thresholds of angles and distance
    matched_data['matches_id'] = matched_data.apply(lambda x: find_best_match(buffer_matches, ref_index=x.name, osm_edges=osm_edges, reference_edge=x['geometry'], angular_threshold=angular_threshold, hausdorff_threshold=hausdorff_threshold), axis=1)
    
    matched_data.dropna(inplace = True)

    # Get ids of matched osm edges
    matched_ids = osm_edges.loc[matched_data.matches_id, 'osmid'].values
    matched_data['osmid'] = matched_ids

    print(f'{len(matched_data)} reference segments were matched to OSM edges')

    print(f'{ len(reference_data) - len(matched_data) } reference segments were not matched')
    
    return matched_data