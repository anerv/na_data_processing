'''
TODO:
- resample to different resolution?
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
from rasterio.merge import merge
from rasterio.mask import mask
from shapely.geometry import box
from rasterio.warp import calculate_default_transform, reproject, Resampling

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
    "crs": pop_src_1.crs.data # TODO
    }
)
merged_fp = '../data/pop/merged_pop_raster.tif'
with rasterio.open(merged_fp, "w", **out_meta) as dest:
    dest.write(mosaic)

# RELOAD
mosaic = rasterio.open(merged_fp)

# CLIP TO DK EXTENT
minx, miny = 6, 54
maxx, maxy = 16, 57.9
bbox = box(minx, miny, maxx, maxy)
geo = gpd.GeoDataFrame({'geometry': bbox}, index=[0], crs='EPSG:4326')
geo = geo.to_crs(crs=pop_src_1.crs.data)

bb_coords = [json.loads(geo.to_json())['features'][0]['geometry']]

clipped, out_transform = mask(mosaic, shapes=bb_coords, crop=True)

out_meta = mosaic.meta.copy()

out_meta.update({
    "driver": "GTiff",
    "height": clipped.shape[1],
    "width": clipped.shape[2],
    "transform": out_trans,
    "crs": pop_src_1.crs.data # TODO
    }
)

clipped_fp = '../data/pop/clipped_pop_raster.tif'
with rasterio.open(clipped_fp, "w", **out_meta) as dest:
    dest.write(clipped)

# REPROJECT
dst_crs = 'EPSG:4326'
proj_fp = '../data/pop/reproj_pop_raster.tif'

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

    with rasterio.open(proj_fp, 'w', **kwargs) as dst:
        for i in range(1, src.count + 1):
            reproject(
                source=rasterio.band(src, i),
                destination=rasterio.band(dst, i),
                src_transform=src.transform,
                src_crs=src.crs,
                dst_transform=transform,
                dst_crs=dst_crs,
                resampling=Resampling.bilinear) # TODO: Should I use average?

#%%

# TODO: figure out min hex resolution I want for population data - make sure that raster data resolution matches this

upscale_factor = 2

with rasterio.open(proj_fp) as dataset:

    # resample data to target shape
    data = dataset.read(
        out_shape=(
            dataset.count,
            int(dataset.height * upscale_factor),
            int(dataset.width * upscale_factor)
        ),
        resampling=Resampling.bilinear
    )

    # scale image transform
    transform = dataset.transform * dataset.transform.scale(
        (dataset.width / data.shape[-1]),
        (dataset.height / data.shape[-2])
    )

# TODO: Combine with H3 data


# TODO: CREATE URBAN/NON-URBAN CLASSIFICATION





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
