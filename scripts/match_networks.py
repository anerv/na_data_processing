#%%
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
engine = dbf.connect_alc(db_name, db_user, db_password, db_port=db_port)

get_geodk = "SELECT * FROM geodk_bike_simple;"

get_osm = 'SELECT osmid, cycling_infrastructure, edge_id, geometry FROM osm_edges_simplified;'

geodk_simplified = gpd.GeoDataFrame.from_postgis(get_geodk, engine, geom_col='geometry' )

osm_edges_simplified = gpd.GeoDataFrame.from_postgis(get_osm, engine, geom_col='geometry')

assert len(geodk_simplified) == len(geodk_simplified['edge_id'].unique())
assert len(osm_edges_simplified) == len(osm_edges_simplified['edge_id'].unique())

#%%
# Get subset
bb = osm_edges_simplified.unary_union.bounds
#geodk_simplified = geodk_simplified.clip(osm_edges_simplified.unary_union.envelope)

geodk_simplified = geodk_simplified.cx[bb[0]:bb[2],bb[1]:bb[3]]

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

osm_cycling_segments = osm_segments.loc[osm_segments.cycling_infrastructure =='yes']
#%%
# Match cycling segments

start = timer()

print('Starting matching...')

cycling_buffer_matches = mf.overlay_buffer(reference_data=ref_segments, osm_data=osm_cycling_segments, ref_id_col='seg_id_ref', osm_id_col='seg_id', dist=15)

print('Buffer matches found!')

# Find segment matches
cycling_segment_matches = mf.find_matches_from_buffer(buffer_matches=cycling_buffer_matches, osm_edges=osm_cycling_segments, reference_data=ref_segments, angular_threshold=30, hausdorff_threshold=17)

print('Feature matching of bike segments completed!')

end = timer()

print(end-start)

matches_fp = f'../data/cycling_segment_matches.pickle'
with open(matches_fp, 'wb') as f:
        pickle.dump(cycling_segment_matches, f)

# Summarize to feature matches
osm_matched_ids, osm_undec = mf.summarize_feature_matches(osm_cycling_segments, cycling_segment_matches,'seg_id','osmid',osm=True)
osm_edges_simplified.loc[osm_edges_simplified.edge_id.isin(osm_matched_ids)].plot();

ref_matched_ids, ref_undec = mf.summarize_feature_matches(ref_segments, cycling_segment_matches, 'seg_id_ref','edge_id',osm=False)
geodk_simplified.loc[geodk_simplified.edge_id.isin(ref_matched_ids)].plot();

#%%
# Get unmatched data
ref_segments_unmatched = ref_segments.loc[~ref_segments.edge_id.isin(ref_matched_ids)]
osm_segments_no_bike = osm_segments.loc[osm_segments.cycling_infrastructure =='no']

ref_segments_unmatched = ref_segments_unmatched.loc[ref_segments_unmatched.highway != 'footway']
ref_segments_foot = ref_segments_unmatched.loc[ref_segments_unmatched.highway == 'footway']
#%%
# Match segments v. 2
print('Starting matching of unmatched segments...')

buffer_matches_unmatched = mf.overlay_buffer(reference_data=ref_segments_unmatched, osm_data=osm_segments_no_bike, ref_id_col='seg_id_ref', osm_id_col='seg_id', dist=15)

print('Buffer matches found!')

# Find segment matches v.2
segment_matches_unmatched = mf.find_matches_from_buffer(buffer_matches=buffer_matches_unmatched, osm_edges=osm_segments_no_bike, reference_data=ref_segments_unmatched, angular_threshold=30, hausdorff_threshold=17)

print('Feature matching round 2 completed!')

matches_fp = f'../data/segment_matches_unmatched.pickle'
with open(matches_fp, 'wb') as f:
        pickle.dump(segment_matches_unmatched, f)


# Summarize to feature matches
osm_matched_ids_2, osm_undec_2 = mf.summarize_feature_matches(osm_segments_no_bike, segment_matches_unmatched,'seg_id','osmid',osm=True)
osm_edges_simplified.loc[osm_edges_simplified.edge_id.isin(osm_matched_ids_2)].plot();

# Summarize
ref_matched_ids_2, ref_undec_2 = mf.summarize_feature_matches(ref_segments_unmatched, segment_matches_unmatched, 'seg_id_ref','edge_id',osm=False)
geodk_simplified.loc[geodk_simplified.edge_id.isin(ref_matched_ids_2)].plot();

#%%
# Match segments - footways
print('Starting matching of unmatched segments...')

buffer_matches_foot = mf.overlay_buffer(reference_data=ref_segments_foot, osm_data=osm_segments_no_bike, ref_id_col='seg_id_ref', osm_id_col='seg_id', dist=15)

print('Buffer matches using footways found!')

# Find segment matches v.2
segment_matches_foot = mf.find_matches_from_buffer(buffer_matches=buffer_matches_unmatched, osm_edges=osm_segments_no_bike, reference_data=ref_segments_foot, angular_threshold=30, hausdorff_threshold=17)

print('Feature matching round 3 completed!')

matches_fp = f'../data/segment_matches_footways.pickle'
with open(matches_fp, 'wb') as f:
        pickle.dump(segment_matches_foot, f)


# Summarize to feature matches
osm_matched_ids_3, osm_undec_3 = mf.summarize_feature_matches(osm_segments_no_bike, segment_matches_foot,'seg_id','osmid',osm=True)
osm_edges_simplified.loc[osm_edges_simplified.edge_id.isin(osm_matched_ids_3)].plot();

# Summarize
ref_matched_ids_3, ref_undec_3 = mf.summarize_feature_matches(ref_segments_foot, segment_matches_foot, 'seg_id_ref','edge_id',osm=False)
geodk_simplified.loc[geodk_simplified.edge_id.isin(ref_matched_ids_3)].plot();


#%%
# Merge matches
segment_matches = pd.concat([cycling_segment_matches, segment_matches_unmatched, segment_matches_foot])

assert len(segment_matches) == len(cycling_segment_matches) + len(segment_matches_unmatched) + len(segment_matches_foot)
assert segment_matches.columns.to_list() == cycling_segment_matches.columns.to_list() == segment_matches_unmatched.columns.to_list() == segment_matches_foot.columns.to_list()
#%%
matches_fp = f'../data/segment_matches_full.pickle'
with open(matches_fp, 'wb') as f:
        pickle.dump(segment_matches, f)

#%%
osm_matched_ids = osm_matched_ids + osm_matched_ids_2
ref_matched_ids = ref_matched_ids + ref_matched_ids_2

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

#%%
updated_osm = mf.update_osm(osm_segments, osm_edges_simplified, segment_matches, 'vejklasse', 'edge_id','seg_id')

#%%
# Export results
# Create dataframe with osm_edge_ids and new attribute value
matched_osm = updated_osm[['edge_id','vejklasse']]

if use_postgres:

    print('Saving data to PostgreSQL!')

    connection = dbf.connect_pg(db_name, db_user, db_password)


    engine = dbf.connect_alc(db_name, db_user, db_password, db_port=db_port)

    drop_table = 'DROP TABLE IF EXISTS osm_matches;'
    create_table = 'CREATE TABLE osm_matches (edge_id VARCHAR, vejklasse VARCHAR);'

    connection = dbf.connect_pg(db_name, db_user, db_password)

    run_drop_table = dbf.run_query_pg(drop_table, connection)
    run_create_table = dbf.run_query_pg(create_table, connection)

    connection = dbf.connect_pg(db_name, db_user, db_password)

    matched_osm.to_sql(name='osm_matches',con=engine, if_exists='replace')

    q = 'SELECT edge_id, vejklasse FROM osm_matches LIMIT 10;'

    test = dbf.run_query_pg(q, connection)

    print(test)

    connection.close()

else:

    print('Saving data to file!')

    matched_osm.set_index('edge_id',inplace=True)
    matched_osm_dict = matched_osm.to_dict('index')

    with open('../results/matched_osm_dict', 'w') as fp:
        json.dump(matched_osm_dict, fp)

#%%

if use_postgres:

    print('Combining OSM and updated data!')

    connection = dbf.connect_pg(db_name, db_user, db_password)

    engine = dbf.connect_alc(db_name, db_user, db_password, db_port=db_port)

    q = 'merge_matched_osm.sql'

    merge = dbf.run_query_pg(q, connection)

    connection = dbf.connect_pg(db_name, db_user, db_password)

    q = 'SELECT edge_id, geodk_bike FROM osm_edges_simplified WHERE geodk_bike IS NOT NULL LIMIT 10;'

    test = dbf.run_query_pg(q, connection)

    print(test)

    q = 'SELECT COUNT(*) FROM osm_edges_simplified WHERE geodk_bike IS NOT NULL;'

    connection = dbf.connect_pg(db_name, db_user, db_password)

    test = dbf.run_query_pg(q, connection)

    print(f'{test[0][0]} edges have information from being matched with geodk bike!')
    
    q1 = "SELECT COUNT(*) FROM osm_edges_simplified WHERE cycling_infrastructure = 'yes';"
    q2 = "SELECT COUNT(*) FROM osm_edges_simplified WHERE cycling_infra_new = 'yes';"

    connection = dbf.connect_pg(db_name, db_user, db_password)

    count1 = dbf.run_query_pg(q1, connection)[0][0]
    count2 = dbf.run_query_pg(q2, connection)[0][0]

    print(f'{count2-count1} new edges are marked as cycling infrastructure!')

    connection.close()


# %%
with open('../data/segment_matches.pickle', 'rb') as fp:
        seg_meg = pickle.load(fp)
# %%
