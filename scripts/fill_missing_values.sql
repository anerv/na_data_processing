-- Surface
ALTER TABLE osm_edges_simplified
    ADD COLUMN surface_as VARCHAR,
    ADD COLUMN lit_as VARCHAR,
    ADD COLUMN speed_as VARCHAR,
    ADD COLUMN 
;

# For all of them, create new col with assumed values
# Only update those with no existing value



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

# Speed!