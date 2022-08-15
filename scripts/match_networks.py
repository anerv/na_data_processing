#%%
import geopandas as gpd
import osmnx as ox
import yaml
import matplotlib.pyplot as plt
import json
import pickle
from src import evaluation_functions as ef
from src import matching_functions as mf
from src import db_functions as dbf

with open(r'config.yml') as file:
    parsed_yaml_file = yaml.load(file, Loader=yaml.FullLoader)

    use_postgres = parsed_yaml_file['use_postgres']

    osm_fp = parsed_yaml_file['osm_fp']

    crs = parsed_yaml_file['CRS']

    db_name = parsed_yaml_file['db_name']
    db_user = parsed_yaml_file['db_user']
    db_password = parsed_yaml_file['db_password']
    db_host = parsed_yaml_file['db_host']
    db_port = parsed_yaml_file['db_port']
  
print('Settings loaded!')

#%%
# Load data
connection = dbf.connect_pg(db_name, db_user, db_password)

get_geodk = "SELECT * FROM geodk_bike_simple;"

get_osm = 'SELECT osmid, edge_id, geometry FROM osm_edges_simplified;'

geodk_simplified = gpd.GeoDataFrame.from_postgis(get_geodk, connection, geom_col='geometry' )

osm_edges_simplified = gpd.GeoDataFrame.from_postgis(get_osm, connection, geom_col='geometry')

assert len(geodk_simplified) == len(geodk_simplified['edge_id'].unique())
assert len(osm_edges_simplified) == len(osm_edges_simplified['edge_id'].unique())

#%%
# Create segments
osm_segments = mf.create_segment_gdf(osm_edges_simplified, segment_length=10)
osm_segments.rename(columns={'osmid':'org_osmid'}, inplace=True)
osm_segments['osmid'] = osm_segments['edge_id'] # Because matching function assumes an id column names osmid as unique id for edges
osm_segments.set_crs(crs, inplace=True)
osm_segments.dropna(subset=['geometry'],inplace=True)

ref_segments = mf.create_segment_gdf(geodk_simplified, segment_length=10)
ref_segments.set_crs(crs, inplace=True)
ref_segments.rename(columns={'seg_id':'seg_id_ref'}, inplace=True) 
ref_segments.dropna(subset=['geometry'],inplace=True)

print('Segments created!')
#%%
# Match segments
print('Starting matching...')

buffer_matches = mf.overlay_buffer(reference_data=ref_segments, osm_data=osm_segments, ref_id_col='seg_id_ref', osm_id_col='seg_id', dist=15)

print('Buffer matches found!')

segment_matches = mf.find_matches_from_buffer(buffer_matches=buffer_matches, osm_edges=osm_segments, reference_data=ref_segments, angular_threshold=30, hausdorff_threshold=17)

print('Feature matching completed!')

matches_fp = f'../data/segment_matches.pickle'
with open(matches_fp, 'wb') as f:
        pickle.dump(segment_matches, f)

#%%
# Summarize to feature matches
osm_matched_ids, osm_undec = mf.summarize_feature_matches(osm_segments, segment_matches,'seg_id','osmid',osm=True)
osm_edges_simplified.loc[osm_edges_simplified.edge_id.isin(osm_matched_ids)].plot();

#%%
ref_matched_ids, ref_undec = mf.summarize_feature_matches(ref_segments, segment_matches, 'seg_id_ref','edge_id',osm=False)
geodk_simplified.loc[geodk_simplified.edge_id.isin(ref_matched_ids)].plot();

#%%
count_matched_osm = len(osm_edges_simplified.loc[osm_edges_simplified.edge_id.isin(osm_matched_ids)])
count_matched_ref = len(geodk_simplified.loc[geodk_simplified.edge_id.isin(ref_matched_ids)])

print(f'Out of {len(osm_edges_simplified)} OSM edges, {count_matched_osm} were matched with a reference edge.')
print(f'Out of {len(geodk_simplified)} reference edges, {count_matched_ref} were matched with an OSM edge.')

length_matched_osm = osm_edges_simplified.loc[osm_edges_simplified.edge_id.isin(osm_matched_ids)].geometry.length.sum()
length_unmatched_osm = osm_edges_simplified.loc[~osm_edges_simplified.edge_id.isin(osm_matched_ids)].geometry.length.sum()

length_matched_ref = geodk_simplified.loc[geodk_simplified.edge_id.isin(ref_matched_ids)].geometry.length.sum()
length_unmatched_ref = geodk_simplified.loc[~geodk_simplified.edge_id.isin(ref_matched_ids)].geometry.length.sum()

print(f'Out of {osm_edges_simplified.geometry.length.sum()/1000:.2f} km of OSM edges, {length_matched_osm/1000:.2f} km were matched with a reference edge.')
print(f'Out of {geodk_simplified.geometry.length.sum()/1000:.2f} km of reference edges, {length_matched_ref/1000:.2f} km were matched with an OSM edge.')
