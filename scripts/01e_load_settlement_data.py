'''
SETTLEMENT (URBAN/RURAL DATA)

rural = [11,12]
semi_rural = [13]
sub_semi_urban = [21,22]
urban = [23,30]

'''

#%%
import matplotlib.pyplot as plt
import rasterio
import geopandas as gpd
import pandas as pd 
import yaml
import matplotlib.pyplot as plt
import json
import pickle
from src import db_functions as dbf
from src import plotting_functions as pf
from rasterio.plot import show
from rasterio.merge import merge
from rasterio.mask import mask
from rasterio.warp import calculate_default_transform, reproject, Resampling
from rasterio.plot import show_hist
import rioxarray as rxr
import h3
from shapely.geometry import Polygon


with open(r'../config.yml') as file:

    parsed_yaml_file = yaml.load(file, Loader=yaml.FullLoader)

    crs = parsed_yaml_file['CRS']

    settlement_fp_1 = parsed_yaml_file['settlement_fp_1']
    settlement_fp_2 = parsed_yaml_file['settlement_fp_2']

    db_name = parsed_yaml_file['db_name']
    db_user = parsed_yaml_file['db_user']
    db_password = parsed_yaml_file['db_password']
    db_host = parsed_yaml_file['db_host']
    db_port = parsed_yaml_file['db_port']
  
print('Settings loaded!')
#%%
# LOAD DATA
sett_src_1 = rasterio.open(settlement_fp_1)
sett_src_2 = rasterio.open(settlement_fp_2)

# MERGE RASTERS
mosaic, out_trans = merge([sett_src_1, sett_src_2])

out_meta = sett_src_1.meta.copy()

out_meta.update({
    "driver": "GTiff",
    "height": mosaic.shape[1],
    "width": mosaic.shape[2],
    "transform": out_trans,
    "crs": sett_src_1.crs
    }
)
merged_fp = '../data/intermediary/settlement/merged_sett_raster.tif'
with rasterio.open(merged_fp, "w", **out_meta) as dest:
    dest.write(mosaic)

merged = rasterio.open(merged_fp)

# Load DK boundaries
engine = dbf.connect_alc(db_name, db_user, db_password, db_port=db_port)

get_muni = 'SELECT navn, kommunekode, geometry FROM muni_boundaries'

muni = gpd.GeoDataFrame.from_postgis(get_muni, engine, geom_col='geometry' )

dissolved = muni.dissolve()
dissolved_buffer = dissolved.buffer(500)

dissolved_proj = dissolved_buffer.to_crs(merged.crs)
convex = dissolved_proj.convex_hull

geo = gpd.GeoDataFrame({'geometry': convex[0]}, index=[0], crs=merged.crs)

coords = [json.loads(geo.to_json())['features'][0]['geometry']]

clipped, out_transform = mask(merged, shapes=coords, crop=True)

out_meta = merged.meta.copy()

out_meta.update({
    "driver": "GTiff",
    "height": clipped.shape[1],
    "width": clipped.shape[2],
    "transform": out_transform,
    "crs": merged.crs
    }
)
clipped_fp = '../data/intermediary/settlement/clipped_sett_raster.tif'
with rasterio.open(clipped_fp, "w", **out_meta) as dest:
    dest.write(clipped)

#%%
# # FILTER OUT NA DATA AND WATER
# clipped_sett = rxr.open_rasterio(clipped_fp)

# # Filter out no data values
# sett_masked = clipped_sett.where(clipped_sett != -200)

# sett_masked.plot.hist()

# # Filter out water values
# sett_masked = sett_masked.where(sett_masked !=10)

# sett_masked.plot.hist()

# sett_masked.plot()

# masked_fp = '../data/intermediary/settlement/sett_masked.tiff'
# sett_masked.rio.to_raster(masked_fp)


# REPROJECT TO CRS USED BY H3
dst_crs = 'EPSG:4326'
proj_fp_wgs84 = '../data/intermediary/settlement/reproj_sett_raster_wgs84.tif'

with rasterio.open(clipped_fp) as src:
    transform, width, height = calculate_default_transform(
        src.crs, dst_crs, src.width, src.height, *src.bounds)
    kwargs = src.meta.copy()
    kwargs.update({
        'crs': dst_crs,
        'transform': transform,
        'width': width,
        'height': height
    })                                                                                                                                                                                                                                                                                                                                                                                                             

    with rasterio.open(proj_fp_wgs84, 'w', **kwargs) as dst:
        for i in range(1, src.count + 1):
            reproject(
                source=rasterio.band(src, i),
                destination=rasterio.band(dst, i),
                src_transform=src.transform,
                src_crs=src.crs,
                dst_transform=transform,
                dst_crs=dst_crs,
                resampling=Resampling.mode) # Using the most often appearing value


test = rasterio.open(proj_fp_wgs84)
assert test.crs.to_string() == 'EPSG:4326'

print('Settlement data has been merged, clipped,and reprojected!')


#%%
# COMBINE WITH H3 DATA
# We use H3 hexagons at level 7, to avoid blank spots in the H3 cover

sett_df = (rxr.open_rasterio(proj_fp_wgs84)
      .sel(band=1)
      .to_pandas()
      .stack()
      .reset_index()
      .rename(columns={'x': 'lng', 'y': 'lat', 0: 'sett_code'}))

sett_df = sett_df[sett_df.sett_code>-200]
sett_df = sett_df[sett_df.sett_code!=10]

sett_gdf = gpd.GeoDataFrame(
    sett_df, geometry=gpd.points_from_xy(sett_df.lng, sett_df.lat))

sett_gdf.set_crs('EPSG:4326',inplace=True)

dk_gdf = gpd.GeoDataFrame({'geometry': dissolved_proj}, crs=dissolved_proj.crs)
dk_gdf.to_crs('EPSG:4326',inplace=True)

sett_gdf = gpd.sjoin(sett_gdf, dk_gdf, op='within', how='inner')
sett_gdf.drop('index_right',axis=1,inplace=True)

pf.plot_scatter(sett_gdf, metric_col='sett_code', marker='.', colormap='Oranges')

#%%
# INDEX SETTLEMENT DATA AT VARIOUS H3 LEVELS
for res in range(6, 10):
    col_hex_id = "hex_id_{}".format(res)
    col_geom = "geometry_{}".format(res)
    msg_ = "At resolution {} -->  H3 cell id : {} and its geometry: {} "
    print(msg_.format(res, col_hex_id, col_geom))

    sett_gdf[col_hex_id] = sett_gdf.apply(
                                        lambda row: h3.geo_to_h3(
                                                    lat = row['lat'],
                                                    lng = row['lng'],
                                                    resolution = res),
                                        axis = 1)

    # use h3.h3_to_geo_boundary to obtain the geometries of these hexagons
    sett_gdf[col_geom] = sett_gdf[col_hex_id].apply(
                                        lambda x: {"type": "Polygon",
                                                   "coordinates":
                                                   [h3.h3_to_geo_boundary(
                                                       h=x, geo_json=True)]
                                                   }
                                         )
#%%
# Convert to H3 polygons
hex_id_col = 'hex_id_7'

grouped = sett_gdf.groupby(hex_id_col)

hex_sett_code = {}

for name, g in grouped:
    hex_sett_code[name] = g.sett_code.value_counts().idxmax()

h3_groups = pd.DataFrame.from_dict(hex_sett_code,orient='index',columns=['sett_code']).reset_index()

h3_groups.rename({'index':hex_id_col},axis=1, inplace=True)

h3_groups['lat'] = h3_groups[hex_id_col].apply(lambda x: h3.h3_to_geo(x)[0])
h3_groups['lng'] = h3_groups[hex_id_col].apply(lambda x: h3.h3_to_geo(x)[1])

h3_groups['hex_geometry'] = h3_groups[hex_id_col].apply(
                            lambda x: {
                                "type": "Polygon",
                                "coordinates":
                                [h3.h3_to_geo_boundary(
                                    h=x, geo_json=True)]
                            }
                )

#%%
h3_groups.plot.scatter(x='lng',y='lat',c='sett_code',marker='o',edgecolors='none',colormap='Oranges',figsize=(30,20))
plt.xticks([], []); plt.yticks([], []);
plt.title('hex-grid: settulation');

#%%
# Create polygon geometries
h3_groups['geometry'] = h3_groups['hex_geometry'].apply(lambda x: Polygon(list(x['coordinates'][0])))

h3_gdf = gpd.GeoDataFrame(h3_groups, geometry='geometry',crs='EPSG:4326')

# Export data
h3_gdf.to_file('../data/intermediary/settlement/h3_7_polygons.gpkg')

#%%
print('Saving data to Postgres!')

connection = dbf.connect_pg(db_name, db_user, db_password)

engine = dbf.connect_alc(db_name, db_user, db_password, db_port=db_port)

dbf.to_postgis(geodataframe=h3_gdf, table_name='settlement_polygons', engine=engine)

q = 'SELECT hex_id_7, sett_code FROM settlement_polygons LIMIT 10;'

test = dbf.run_query_pg(q, connection)

print(test)

connection.close()
# %%
