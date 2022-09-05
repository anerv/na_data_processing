 
import geopandas as gpd
import osmnx as ox
import pandas as pd
import yaml
import matplotlib.pyplot as plt
import json
import pickle
from src import matching_functions as mf
from src import db_functions as dbf
from timeit import default_timer as timer

with open(r'../config.yml') as file:
    parsed_yaml_file = yaml.load(file, Loader=yaml.FullLoader)

    crs = parsed_yaml_file['CRS']

  
print('Settings loaded!')

 
# TODO: UPDATE

# Load data

get_geodk = "SELECT * FROM geodk_bike;"

get_osm = 'SELECT osmid, cycling_infrastructure, highway, edge_id, geometry FROM osm_edges_simplified;'

osm_edges_simplified = None
geodk = None
 
# Create segments
osm_segments = mf.create_segment_gdf(osm_edges_simplified, segment_length=10)
osm_segments.rename(columns={'osmid':'org_osmid'}, inplace=True)
osm_segments['osmid'] = osm_segments['edge_id'] # Because matching function assumes an id column names osmid as unique id for edges
osm_segments.set_crs(crs, inplace=True)
osm_segments.dropna(subset=['geometry'],inplace=True)

ref_segments = mf.create_segment_gdf(geodk, segment_length=10)
ref_segments.set_crs(crs, inplace=True)
ref_segments.rename(columns={'seg_id':'seg_id_ref'}, inplace=True) 
ref_segments.dropna(subset=['geometry'],inplace=True)

print('Segments created!')

# MATCH CYCLING SEGMENTS
osm_cycling_segments = osm_segments.loc[osm_segments.cycling_infrastructure =='yes'] # Get cycling segments for first matching process

cycling_buffer_matches = mf.overlay_buffer(reference_data=ref_segments, osm_data=osm_cycling_segments, ref_id_col='seg_id_ref', osm_id_col='seg_id', dist=15)

# Find segment matches
cycling_segment_matches = mf.find_matches_from_buffer(buffer_matches=cycling_buffer_matches, osm_edges=osm_cycling_segments, reference_data=ref_segments, angular_threshold=30, hausdorff_threshold=17)

matches_fp = f'../data/cycling_segment_matches.pickle'
with open(matches_fp, 'wb') as f:
        pickle.dump(cycling_segment_matches, f)

# Summarize to feature matches
osm_matched_ids, osm_undec = mf.summarize_feature_matches(osm_cycling_segments, cycling_segment_matches,'seg_id','osmid',osm=True)
osm_edges_simplified.loc[osm_edges_simplified.edge_id.isin(osm_matched_ids)].plot();

ref_matched_ids, ref_undec = mf.summarize_feature_matches(ref_segments, cycling_segment_matches, 'seg_id_ref','edge_id',osm=False)
geodk.loc[geodk.edge_id.isin(ref_matched_ids)].plot();

print('Matches summarized!')
 
# Get unmatched data
ref_segments_unmatched = ref_segments.loc[~ref_segments.edge_id.isin(ref_matched_ids)]

# OSM segments not from cycling infra
osm_segments_no_bike = osm_segments.loc[osm_segments.cycling_infrastructure =='no']

 
# MATCH REMAINING SEGMENTS
buffer_matches_unmatched = mf.overlay_buffer(reference_data=ref_segments_unmatched, osm_data=osm_segments_no_bike, ref_id_col='seg_id_ref', osm_id_col='seg_id', dist=15)

# Find segment matches v.2
segment_matches_unmatched = mf.find_matches_from_buffer(buffer_matches=buffer_matches_unmatched, osm_edges=osm_segments_no_bike, reference_data=ref_segments_unmatched, angular_threshold=30, hausdorff_threshold=17)

matches_fp = f'../data/segment_matches_unmatched.pickle'
with open(matches_fp, 'wb') as f:
        pickle.dump(segment_matches_unmatched, f)

# Summarize to feature matches
osm_matched_ids_2, osm_undec_2 = mf.summarize_feature_matches(osm_segments_no_bike, segment_matches_unmatched,'seg_id','osmid',osm=True)
osm_edges_simplified.loc[osm_edges_simplified.edge_id.isin(osm_matched_ids_2)].plot();

# Summarize
ref_matched_ids_2, ref_undec_2 = mf.summarize_feature_matches(ref_segments_unmatched, segment_matches_unmatched, 'seg_id_ref','edge_id',osm=False)
geodk.loc[geodk.edge_id.isin(ref_matched_ids_2)].plot();
 
# Merge matches
segment_matches = pd.concat([cycling_segment_matches, segment_matches_unmatched])

assert len(segment_matches) == len(cycling_segment_matches) + len(segment_matches_unmatched)
assert segment_matches.columns.to_list() == cycling_segment_matches.columns.to_list() == segment_matches_unmatched.columns.to_list()

matches_fp = f'../data/segment_matches_full.pickle'
with open(matches_fp, 'wb') as f:
        pickle.dump(segment_matches, f)

 
# Summarize matches with based on attributes
updated_osm_vejklasse = mf.update_osm(osm_segments, osm_edges_simplified, segment_matches, 'vejklasse', 'edge_id','seg_id')

updated_osm_overflade = mf.update_osm(osm_segments, osm_edges_simplified, segment_matches, 'overflade', 'edge_id','seg_id')

 
# EXPORT RESULTS

# Create dataframe with osm_edge_ids and new attribute value
matched_osm_vejklasse = updated_osm_vejklasse[['edge_id','vejklasse']]
matched_osm_overflade = updated_osm_overflade[['edge_id','overflade']]

print('Saving data to file!')

matched_osm_vejklasse.set_index('edge_id',inplace=True)
matched_osm_roadclass_dict = matched_osm_vejklasse.to_dict('index')

with open('../results/matched_osm_roadclass', 'w') as fp:
        json.dump(matched_osm_roadclass_dict, fp)

matched_osm_overflade.set_index('edge_id',inplace=True)
matched_osm_overflade_dict = matched_osm_vejklasse.to_dict('index')

with open('../results/matched_osm_surface', 'w') as fp:
        json.dump(matched_osm_overflade_dict, fp)

