'''
Script for matching road networks.
'''
# TODO: Docs

# TODO: Functionality for doing analysis grid by grid

#%%
import pickle
import geopandas as gpd
import pandas as pd
import yaml
from src import db_functions as dbf
from src import matching_functions as mf
import osmnx as ox
import matplotlib.pyplot as plt
import os.path
#%%
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
if use_postgres:
    
    print('Connecting to DB!')
    connection = dbf.connect_pg(db_name, db_user, db_password)

    get_osm = '''SELECT * FROM osm_edges WHERE highway IN ('residential', 'service', 'primary', 'tertiary',
        'tertiary_link', 'secondary', 'cycleway', 'path', 'living_street',
        'unclassified', 'primary_link', 'motorway_link', 'motorway', 'track',
        'secondary_link', 'pathway', 'trunk_link', 'trunk');'''

    get_osm_nodes = 'SELECT * FROM osm_nodes;'

    get_geodk = 'SELECT * FROM geodk_bike;'

    get_grid = 'SELECT * FROM grid;'

    osm_edges = gpd.GeoDataFrame.from_postgis(get_osm, connection, geom_col='geometry' )

    osm_nodes = gpd.GeoDataFrame.from_postgis(get_osm_nodes, geom_col='geometry') #TODO: Only keep nodes used by filtered osm_edges

    reference_data = gpd.GeoDataFrame.from_postgis(get_geodk, connection, geom_col='geometry' )

    grid = gpd.GeoDataFrame.from_postgis(get_grid, connection, geom_col='geometry')

else:

    print('Loading files!')
  
    with open('../data/osm_edges_sim.pickle', 'rb') as fp:
        osm_edges = pickle.load(fp)

    with open('../data/reference_data.pickle', 'rb') as fp:
        reference_data = pickle.load(fp)

    with open('../data/osm_nodes_sim.pickle', 'rb') as fp:
        osm_nodes = pickle.load(fp)

    
    highway_values = ['residential', 'service', 'primary', 'tertiary',
            'tertiary_link', 'secondary', 'cycleway', 'path', 'living_street',
            'unclassified', 'primary_link', 'motorway_link', 'motorway', 'track',
            'secondary_link', 'pathway', 'trunk_link', 'trunk'
            ]

    osm_edges = osm_edges.loc[osm_edges['highway'].isin(highway_values)]

    reference_data = reference_data.loc[reference_data['vejklasse'].isin(['Cykelbane langs vej', 'Cykelsti langs vej'])]


assert osm_edges.crs == crs
assert reference_data.crs == crs

print(f'Number of rows in osm_edge table: {len(osm_edges)}')
print(f'Number of rows in reference_data table: {len(reference_data)}')

#%%
#ref = gpd.read_file('../tests/geodk_small_test.gpkg')
#osm = gpd.read_file('../tests/osm_small_test.gpkg')
#%%
if os.path.exists('../data/ref_segments_full.gpkg'):
    ref_segments = gpd.read_file('../data/ref_segments_full.gpkg')

else:
    # Create segments
    ref_segments = mf.create_segment_gdf(reference_data, segment_length=10, id_col=org_ref_id_col)
    ref_segments.set_crs(crs, inplace=True)
    ref_segments.to_file('../data/ref_segments_full.gpkg', driver='GPKG')

#%%
if os.path.exists('../data/osm_segments_full.gpkg'):
    osm_segments = gpd.read_file('../data/osm_segments_full.gpkg')

else:
    # Only segmentize OSM edges within buffer distance from reference edges!
    ref_buffer = reference_data.copy()
    ref_buffer.geometry = ref_buffer.geometry.buffer(distance=15)
    osm_sindex = osm_edges.sindex

    matches_ix = []

    for index, row in ref_buffer.iterrows():
            buffer = row['geometry']
            possible_matches_index = list(osm_sindex.intersection(buffer.bounds))
            possible_matches = osm_edges.iloc[possible_matches_index]
            precise_matches = possible_matches[possible_matches.intersects(buffer)]
            precise_matches_index = list(precise_matches.index)
            matches_ix.extend(precise_matches_index)

    matches_unique = list(set(matches_ix))
    osm_subset = osm_edges.loc[matches_unique]

    osm_segments = mf.create_segment_gdf(osm_subset, segment_length=10, id_col='osmid')
    osm_segments['org_osmid'] = osm_segments.osmid
    osm_segments.rename(columns={'seg_id':'osmid'}) # Because function assumes an id column names osmid
    osm_segments.set_crs(crs, inplace=True)
    osm_segments.to_file('../data/osm_segments_full.gpkg', driver='GPKG')

#%%
# Get smaller subsets
xmin = 723371
xmax = xmin + 2000
ymin = 6180833
ymax = ymin + 2000

osm_segments = osm_segments.cx[xmin:xmax, ymin:ymax].copy(deep=True)
ref_segments = ref_segments.cx[xmin:xmax, ymin:ymax].copy(deep=True)

#%%
buffer_matches = mf.find_matches_buffer(reference_data=ref_segments, osm_data=osm_segments, ref_id_col='seg_id', dist=15)
#%%
final_matches = mf.find_matches_from_buffer(buffer_matches=buffer_matches, osm_edges=osm_segments, reference_data=ref_segments, ref_id_col='seg_id', hausdorff_threshold=17, angular_threshold=30)
#%%
osm_updated = mf.update_osm(osm_segments=osm_segments, ref_segments=ref_segments, osm_data=osm_edges, reference_data=reference_data, final_matches=final_matches, attr='vejklasse',org_ref_id_col=org_ref_id_col)

#%%

'''
id_matches = final_matches.seg_id.to_list()
matched = ref_segments.loc[ref_segments.seg_id.isin(id_matches)]
not_matched = ref_segments.loc[~ref_segments.seg_id.isin(id_matches)]

matched = matched.merge(final_matches, on='seg_id')
matched = matched.astype({org_ref_id_col:'int64','seg_id':'int64','osmid':'int64','osm_index':'int64'})
#
osm_id_matches = final_matches.osmid.to_list()
osm_matched = osm_segments.loc[osm_segments.osmid.isin(osm_id_matches)]
osm_matched.to_file('../data/osm_matched.gpkg', driver='GPKG')

matched.to_file('../data/matched.gpkg', driver='GPKG')
not_matched.to_file('../data/not_matched.gpkg', driver='GPKG')

ref_segments.to_file('../data/ref_segments_test.gpkg', driver='GPKG')
osm_segments.to_file('../data/osm_segments_test.gpkg', driver='GPKG')
'''

#%% 
# TODO: Quality check (optional)
# If you have data on correct matches - check result against this and compute score

if quality_check:
    # Check how many where correct compared to reference data
    # Compute percentage of correct
    # Compute percentage of not matched
    # Compute percentage of wrongly matched
    pass

#%%
#######################################################
if use_postgres:

    # Upload result to DB
    engine = dbf.connect_alc(db_name, db_user, db_password, db_port=db_port)

    dbf.to_postgis(geodataframe=osm_updated, table_name='osm_updated', engine=engine)

    dbf.to_postgis(geodataframe=final_matches, table_name='final_matches', engine=engine)

    connection.close()

else:
    with open('../data/osm_reference_match.pickle', 'wb') as handle:
        pickle.dump(final_matches, handle, protocol=pickle.HIGHEST_PROTOCOL)

    with open('../data/osm_updated.pickle', 'wb') as handle:
        pickle.dump(osm_updated, handle, protocol=pickle.HIGHEST_PROTOCOL)
#%%
