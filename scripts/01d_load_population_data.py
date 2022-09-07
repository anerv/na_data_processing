'''
POPULATION H3 HEX GRID
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
from src import plotting_functions as pf
from rasterio.plot import show
from rasterio.merge import merge
from rasterio.mask import mask
from rasterio.warp import calculate_default_transform, reproject, Resampling
import rioxarray

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
# LOAD DATA
pop_src_1 = rasterio.open(pop_fp_1)
pop_src_2 = rasterio.open(pop_fp_2)

# MERGE RASTERS
mosaic, out_trans = merge([pop_src_1, pop_src_2])

out_meta = pop_src_1.meta.copy()

out_meta.update({
    "driver": "GTiff",
    "height": mosaic.shape[1],
    "width": mosaic.shape[2],
    "transform": out_trans,
    "crs": pop_src_1.crs
    }
)
merged_fp = '../data/intermediary/pop/merged_pop_raster.tif'
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
clipped_fp = '../data/intermediary/pop/clipped_pop_raster.tif'
with rasterio.open(clipped_fp, "w", **out_meta) as dest:
    dest.write(clipped)


# RESAMPLE TO GRID SIZE OF 400 meters

downscale_factor = 1/4

with rasterio.open(clipped_fp) as dataset:

    # resample data to target shape
    resampled = dataset.read(
        out_shape=(
            dataset.count,
            int(dataset.height * downscale_factor),
            int(dataset.width * downscale_factor)
        ),
        resampling=Resampling.bilinear
    )

    # scale image transform
    out_transform = dataset.transform * dataset.transform.scale(
        (dataset.width / resampled.shape[-1]),
        (dataset.height / resampled.shape[-2])
    )

out_meta.update({
    "driver": "GTiff",
    "height": resampled.shape[1],
    "width": resampled.shape[2],
    "transform": out_transform,
    "crs": merged.crs
    }
)

resamp_fp = '../data/intermediary/pop/resampled_pop_raster.tif'
with rasterio.open(resamp_fp, "w", **out_meta) as dest:
    dest.write(resampled)

test = rasterio.open(resamp_fp)
assert round(test.res[0]) == 400
assert round(test.res[1]) == 400


# REPROJECT TO CRS USED BY H3
dst_crs = 'EPSG:4326'
proj_fp_wgs84 = '../data/intermediary/pop/reproj_pop_raster_wgs84.tif'

with rasterio.open(resamp_fp) as src:
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
                resampling=Resampling.bilinear)


test = rasterio.open(proj_fp_wgs84)
assert test.crs.to_string() == 'EPSG:4326'

print('Population data has been merged, clipped, reprojected and downsampled!')

# COMBINE WITH H3 DATA

pop_df = (rioxarray.open_rasterio(proj_fp_wgs84)
      .sel(band=1)
      .to_pandas()
      .stack()
      .reset_index()
      .rename(columns={'x': 'lng', 'y': 'lat', 0: 'population'}))

# Ignore no data values
pop_df = pop_df[pop_df.population>-200]

pop_gdf = gpd.GeoDataFrame(
    pop_df, geometry=gpd.points_from_xy(pop_df.lng, pop_df.lat))

pop_gdf.set_crs('EPSG:4326',inplace=True)

dk_gdf = gpd.GeoDataFrame({'geometry': dissolved_proj}, crs=dissolved_proj.crs)
dk_gdf.to_crs('EPSG:4326',inplace=True)

pop_gdf = gpd.sjoin(pop_gdf, dk_gdf, op='within', how='inner')

pf.plot_scatter(pop_gdf, metric_col='population', marker='.', colormap='Oranges')


# INDEX POPULATION AT VARIOUS H3 LEVELS
for res in range(7, 11):
    col_hex_id = "hex_id_{}".format(res)
    col_geom = "geometry_{}".format(res)
    msg_ = "At resolution {} -->  H3 cell id : {} and its geometry: {} "
    print(msg_.format(res, col_hex_id, col_geom))

    pop_gdf[col_hex_id] = pop_gdf.apply(
                                        lambda row: h3.geo_to_h3(
                                                    lat = row['lat'],
                                                    lng = row['lng'],
                                                    resolution = res),
                                        axis = 1)

    # use h3.h3_to_geo_boundary to obtain the geometries of these hexagons
    pop_gdf[col_geom] = pop_gdf[col_hex_id].apply(
                                        lambda x: {"type": "Polygon",
                                                   "coordinates":
                                                   [h3.h3_to_geo_boundary(
                                                       h=x, geo_json=True)]
                                                   }
                                         )

pop_gdf.to_file('../data/intermediary/pop/pop_hex.gpkg',driver='GPKG')
#%%
# Test plot
hex_id_col = 'hex_id_7'
grouped = pop_gdf.groupby(hex_id_col)['population'].sum().to_frame('population').reset_index()

grouped['lat'] = grouped[hex_id_col].apply(lambda x: h3.h3_to_geo(x)[0])
grouped['lng'] = grouped[hex_id_col].apply(lambda x: h3.h3_to_geo(x)[1])

grouped['hex_geometry'] = grouped[hex_id_col].apply(
                            lambda x: {
                                "type": "Polygon",
                                "coordinates":
                                [h3.h3_to_geo_boundary(
                                    h=x, geo_json=True)]
                            }
                )

grouped.plot.scatter(x='lng',y='lat',c='population',marker='o',edgecolors='none',colormap='Oranges',figsize=(30,20))
plt.xticks([], []); plt.yticks([], []);
plt.title('hex-grid: population');

