'''
Script for loading GeoDK data (shapefile, geopackage etc.) to PostGIS db
Required an existing db with postgis extension
'''
#%%
import geopandas as gpd
import yaml
from src import db_functions as dbf
# %%
with open(r'config.yml') as file:
    parsed_yaml_file = yaml.load(file, Loader=yaml.FullLoader)

    use_postgres = parsed_yaml_file['use_postgres']

    geodk_fp = parsed_yaml_file['geodk_fp']
    geodk_id_col = parsed_yaml_file['geodk_id_col']

    crs = parsed_yaml_file['CRS']

    db_name = parsed_yaml_file['db_name']
    db_user = parsed_yaml_file['db_user']
    db_password = parsed_yaml_file['db_password']
    db_host = parsed_yaml_file['db_host']
    db_port = parsed_yaml_file['db_port']
  
print('Settings loaded!')
#%%
geodk = gpd.read_file(geodk_fp)

geodk.columns = geodk.columns.str.lower()

useful_cols = ['fot_id', 'mob_id', 'feat_kode', 'feat_type', 'featstatus',
       'geomstatus', 'startknude', 'slutknude', 'niveau', 'overflade',
       'rund_koer', 'kom_kode', 'vejkode', 'tilfra_koe',
       'trafikart', 'vejklasse', 'vej_mynd', 'vej_type', 'geometry']

geodk = geodk[useful_cols]

geodk = geodk.to_crs(crs)

assert geodk.crs == crs

assert len(geodk) == len(geodk[geodk_id_col].unique())

#%%
# Get cycling infrastructure
geodk_bike = geodk.loc[geodk.vejklasse.isin(['Cykelsti langs vej','Cykelbane langs vej'])].copy()

# Create unique edge id column
geodk_bike['edge_id'] = geodk_bike.fot_id

assert len(geodk_bike.edge_id) == len(geodk_bike)
#%%
connection = dbf.connect_pg(db_name, db_user, db_password)

engine = dbf.connect_alc(db_name, db_user, db_password, db_port=db_port)

dbf.to_postgis(geodataframe=geodk_bike, table_name='geodk_bike', engine=engine)

q = 'SELECT edge_id, vejklasse FROM geodk_bike LIMIT 10;'

test = dbf.run_query_pg(q, connection)

print(test)

connection.close()

#%%
# with open('../data/reference_data.pickle', 'wb') as handle:
#     pickle.dump(graph_ref, handle, protocol=pickle.HIGHEST_PROTOCOL)

# with open('../data/reference_data_simple.pickle', 'wb') as handle:
#     pickle.dump(G_sim, handle, protocol=pickle.HIGHEST_PROTOCOL)
    
#%%