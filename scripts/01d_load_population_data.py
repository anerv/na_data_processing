'''
Script should load population data to database??
Create H3 polygons with population data
Classify as urban/non-urban
Save H3 polygons to be used to index network for analysis
Save urban/non-urban in a way that allows for classifying network
'''

#%%
import h3
import matplotlib.pyplot as plt

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