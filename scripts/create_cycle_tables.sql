DROP TABLE IF EXISTS geodk_bike_simple;
CREATE TABLE geodk_bike_simple AS
    SELECT * FROM vm_brudt_simple WHERE vejklasse IN ('Cykelbane langs vej', 'Cykelsti langs vej')
;


DROP TABLE IF EXISTS osm_bike;
CREATE TABLE osm_bike AS
    SELECT * FROM osm_edges WHERE 
        highway = 'cycleway' OR
        highway = 'living_street' OR
        cycleway IN ('lane','track','opposite_lane','opposite_track','shared_lane','designated','crossing','share_busway') OR
        cycleway_left IN ('lane','track','opposite_lane','opposite_track','shared_lane','designated','crossing','share_busway') OR 
        cycleway_right IN ('lane','track','opposite_lane','opposite_track','shared_lane','designated','crossing','share_busway') OR
        cycleway_both IN ('lane','track','opposite_lane','opposite_track','shared_lane','designated','crossing','share_busway') OR
        highway = 'track' AND bicycle IN ('designated','yes') OR
        highway = 'service' AND (bicycle = 'designated' or motor_vehicle ='no') OR
        highway = 'path' AND bicycle IN ('designated','yes') OR
        --cyclestreet = 'yes' OR
        bicycle_road = 'yes'
;


DROP TABLE IF EXISTS osm_no_bike;
CREATE TABLE osm_no_bike AS
    SELECT * FROM osm_edges WHERE NOT EXISTS (
        SELECT * FROM osm_bike where osm_bike.osmid = osm_edges.osmid
);

DELETE FROM osm_no_bike WHERE bicycle = 'use_sidepath';