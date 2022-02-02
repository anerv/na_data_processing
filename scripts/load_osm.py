'''
The purpose of this script is to
1) Convert OSM data to a graph format. This step is using pyrosm which has been optimized for loading OSM data from pbf files for large areas
2) Load the resulting data to a PostGIS database 

'''
#%%
import pyrosm
import psycopg2 as pg
import geopandas as gpd
import yaml
#%%

with open(r'config.yml') as file:
    parsed_yaml_file = yaml.load(file, Loader=yaml.FullLoader)


    study_area = parsed_yaml_file['study_area']
    osm_fp = parsed_yaml_file['OSM_fp']
  

    
print('Settings loaded!')
#load osm for test area using pyro osm

