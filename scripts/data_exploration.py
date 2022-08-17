#%%
import folium
import geopandas as gpd
import pandas as pd
import contextily as cx
from src import plotting_functions as pf
import yaml
import pandas as pd
import json
from src import db_functions as dbf
import pickle
from timeit import default_timer as timer
import os.path
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
folium_layers = {
        'Google Satellite': folium.TileLayer(
                tiles = 'https://mt1.google.com/vt/lyrs=s&x={x}&y={y}&z={z}',
                attr = 'Google',
                name = 'Google Satellite',
                overlay = True,
                control = True,
                show = False
                ),
        'whiteback': folium.TileLayer(
                tiles = 'https://api.mapbox.com/styles/v1/krktalilu/ckrdjkf0r2jt217qyoai4ndws/tiles/256/{z}/{x}/{y}@2x?access_token=pk.eyJ1Ijoia3JrdGFsaWx1IiwiYSI6ImNrcmRqMXdycTB3NG8yb3BlcGpiM2JkczUifQ.gEfOn5ttzfH5BQTjqXMs3w',
                name = 'Background: White',
                attr = 'Mapbox',
                control = True,
                overlay = True,
                show = False
                ),
        'Stamen TonerLite': folium.TileLayer(
                tiles = 'https://stamen-tiles-{s}.a.ssl.fastly.net/toner-lite/{z}/{x}/{y}{r}.png',
                attr = 'Map tiles by <a href="http://stamen.com">Stamen Design</a>, <a href="http://creativecommons.org/licenses/by/3.0">CC BY 3.0</a> &mdash; Map data &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
                name = 'Stamen TonerLite',
                control = True,
                overlay = True,
                show = False
        ), 
        'CyclOSM': folium.TileLayer(
                tiles = 'https://{s}.tile-cyclosm.openstreetmap.fr/cyclosm/{z}/{x}/{y}.png',
                attr = 'Map data &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
                name = 'CyclOSM',
                control = True,
                overlay = True,
                show = False
        ),     
        'OSM': folium.TileLayer(
                tiles = 'openstreetmap', 
                name = 'OpenStreetMap',
                attr = 'Map data &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
                control = True, 
                overlay = True
                )
}
#%%
# Read OSM data from DB

connection = dbf.connect_pg(db_name, db_user, db_password)

get_osm1 = "SELECT osmid, cycling_infrastructure, cycling_infra_new, edge_id, geometry FROM osm_edges_simplified WHERE cycling_infrastructure = 'yes';"
get_osm2 = "SELECT osmid, cycling_infrastructure, cycling_infra_new, edge_id, geometry FROM osm_edges_simplified WHERE cycling_infra_new = 'yes';"

cycling_infra = gpd.GeoDataFrame.from_postgis(get_osm1, connection, geom_col='geometry')
cycling_infra2 = gpd.GeoDataFrame.from_postgis(get_osm2,connection, geom_col='geometry')
#%%
def style_function(x, color='purple', weight=3):
    return {"color":color, "weight":weight}

cycling_feat = folium.features.GeoJson(
        cycling_infra, name="cycling_infra",
        style_function=lambda x: style_function(x, color='red'),
        tooltip=folium.GeoJsonTooltip(fields= ["cycling_infrastructure"],labels=True)
        )

cycling_feat2 = folium.features.GeoJson(
        cycling_infra, name="cycling_infra_2",
        style_function=lambda x: style_function(x, color='green'),
        #tooltip=folium.GeoJsonTooltip(fields= ["cycling_infrastructure"],labels=True)
        )

#%%
m = pf.make_foliumplot(
    layers_dict = folium_layers,
    center_gdf = cycling_infra,
    center_crs = cycling_infra.crs,
    feature_layer=[cycling_feat,cycling_feat2]
)
#%%
m.save('../tests/data_expl.html')
#%%