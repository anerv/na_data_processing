'''
TODO:
- load pop data
- merge tif files
- reproject
- resample to different resolution?
- cut to DK area
- create H3 polygons at resolution XX
- Classify as urban/non-urban
- Convert to polygons classified as urban/non-urban
    - load to postgres
- Save H3 polygons with pop densities to file (for later use)

'''

#%%
import h3
import matplotlib.pyplot as plt
import rasterio
import geopandas as gpd
import pandas as pd
import yaml
import matplotlib.pyplot as plt
import json
import pickle
from src import db_functions as dbf
from timeit import default_timer as timer
from rasterio.plot import show

with open(r'../config.yml') as file:

    parsed_yaml_file = yaml.load(file, Loader=yaml.FullLoader)

    crs = parsed_yaml_file['CRS']

    pop_fp_1 = parsed_yaml_file['pop_fp_1']
    pop_fp_2 = parsed_yaml_file['pop_fp_2']


    db_name = parsed_yaml_file['db_name']
    db_user = parsed_yaml_file['db_user']
    db_password = parsed_yaml_file['db_password']
    db_host = parsed_yaml_file['db_host']
    db_port = parsed_yaml_file['db_port']
  
print('Settings loaded!')
#%%

pop_src_1 = rasterio.open(pop_fp_1)
pop_src_2 = rasterio.open(pop_fp_2)


fig, ax = plt.subplots(1, figsize= (12, 12))

#%%

APERTURE_SIZE = 9
hex_col = 'hex'+str(APERTURE_SIZE)

# find hexs containing the points
df[hex_col] = df.apply(lambda x: h3.geo_to_h3(x.lat,x.lng,APERTURE_SIZE),1)

# calculate elevation average per hex
df_dem = df.groupby(hex_col)['elevation'].mean().to_frame('elevation').reset_index()

#find center of hex for visualization
df_dem['lat'] = df_dem[hex_col].apply(lambda x: h3.h3_to_geo(x)[0])
df_dem['lng'] = df_dem[hex_col].apply(lambda x: h3.h3_to_geo(x)[1])

# plot the hexes
plot_scatter(df_dem, metric_col='elevation', marker='o')
plt.title('hex-grid: elevation');
#%%
