'''
Classifies all OSM edges and fills out missing values
'''

#%%
import yaml
from src import db_functions as dbf

#%%

with open(r'config.yml') as file:
    parsed_yaml_file = yaml.load(file, Loader=yaml.FullLoader)

    use_postgres = parsed_yaml_file['use_postgres']

    osm_fp = parsed_yaml_file['osm_fp']
    geodk_fp = parsed_yaml_file['geodk_fp']

    crs = parsed_yaml_file['CRS']

    db_name = parsed_yaml_file['db_name']
    db_user = parsed_yaml_file['db_user']
    db_password = parsed_yaml_file['db_password']
    db_host = parsed_yaml_file['db_host']
    db_port = parsed_yaml_file['db_port']
  
print('Settings loaded!')
#%%
# For all of them, create new col with assumed values
# Only update those with no existing value

# Langs med vej
# All those where it is a tagged to a highway for cars --> along street

# If not along street - I need to look at land use?

# Protected
# Track --> protected
# If its a path with no car traffic/i.e. not along the street --> protected
# Otherwise, unprotected
# Also look at GeoDK


# Surface
# Here we look at the type of highway - all regular streets assumed to be some type of hard surface
# If GEODK says that there is cycling infra - also hard surface
# If its a track along a street - hard surface 

# If its NOT along a street and nothing is tagged - we assume unpaved


# Light
# If its in a city and along a street --> we assume light
# otherwise no light is assumed

# Intersections
# Step one is to detect intersections
# Nodes with degrees more than 2?
# Classify intersections as signalled/controlled or not
# Classify edges as having a problematic or unproblematic intersection?