
# Workflow for running local Valhalla server
# Requires Docker

# https://ikespand.github.io/posts/meili/
docker login docker.pkg.github.com
docker pull docker.pkg.github.com/gis-ops/docker-valhalla/valhalla:latest

mkdir custom_files
wget -O custom_files/denmark-latest.osm.pbf https://download.geofabrik.de/europe/denmark-latest.osm.pbf
docker run --name valhalla_gis-ops -p 8002:8002 -v $PWD/custom_files:/custom_files gisops/valhalla:latest

# https://towardsdatascience.com/map-matching-done-right-using-valhallas-meili-f635ebd17053
# https://medium.com/@hernandezgalaviz/using-valhalla-map-matching-to-get-route-and-travelled-distance-from-raw-gps-points-4ea6b1c88a4c
# https://github.com/gis-ops/docker-valhalla#environment-variables