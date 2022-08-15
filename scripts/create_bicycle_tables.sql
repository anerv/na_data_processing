ALTER TABLE osm_edges
    ADD COLUMN cycling_infrastructure VARCHAR DEFAULT NULL,
    ADD COLUMN cycling_allowed VARCHAR DEFAULT NULL,
    ADD COLUMN pedestrian_allowed VARCHAR DEFAULT NULL,
    ADD COLUMN bike_separated VARCHAR DEFAULT NULL
;

UPDATE osm_edges 
    SET cycling_allowed = 'yes' 
        WHERE bicycle IN ('permissive', 'ok', 'allowed', 'designated')
        OR highway = 'cycleway'
        OR "cycling_infrastructure" = 'yes'
;

UPDATE osm_edges 
    SET cycling_allowed = 'no'
        WHERE bicycle IN ('no', 'dismount', 'use_sidepath')
        OR (road_type = 'motorvej' AND cycling_infrastructure IS NULL)
;

-- Segments where pedestrians are allowed
UPDATE osm_edges 
    SET pedestrian_allowed = 'yes' 
        WHERE highway in ('pedestrian', 'path', 'footway', 'steps')
        OR foot IN ('yes', 'designated', 'permissive', 'official', 'destination')
        OR sidewalk IN ('both', 'left', 'right')
;

-- Cycling infrastructure separated from car street network
UPDATE osm_edges
    SET bike_separated = 'true' 
        WHERE highway = 'cycleway'; -- TODO: Also include paths, trails here

UPDATE osm_edges
    SET bike_separated = 'false' 
        WHERE cycling_infrastructure = 'yes' AND highway != 'cycleway'; -- TODO: Also include paths, trails here

--Determining whether the segment of cycling infrastructure runs along a street or not
-- Along a street with car traffic
UPDATE edges SET along_street = 'true' WHERE car_traffic = 'yes' AND cycling_infrastructure = 'yes';

-- Capturing cycleways digitized as individual ways both still running parallel to a street
CREATE VIEW cycleways AS 
    (SELECT name, highway, road_type, cycling_infrastructure, along_street FROM osm_edges
        WHERE highway = 'cycleway');

CREATE VIEW car_roads AS 
    (SELECT name, highway, road_type, geom FROM osm_edges
        WHERE car_traffic = 'yes');

UPDATE cycleways c SET along_street = 'true'
    FROM car_roads cr WHERE c.name = cr.name;

DROP VIEW cycleways;
DROP VIEW car_roads;

-- TODO: Join geodk on osm based on matches - create new column with content from vejklasse
-- both on osm edges and osm edges simplified?

-- UPDATE TABLE osm_edges
--     SET COLUMN cycling_infrastructure = true
--     WHERE         
--         highway = 'cycleway' OR
--         highway = 'living_street' OR
--         cycleway IN ('lane','track','opposite_lane','opposite_track','shared_lane','designated','crossing','share_busway') OR
--         cycleway_left IN ('lane','track','opposite_lane','opposite_track','shared_lane','designated','crossing','share_busway') OR 
--         cycleway_right IN ('lane','track','opposite_lane','opposite_track','shared_lane','designated','crossing','share_busway') OR
--         cycleway_both IN ('lane','track','opposite_lane','opposite_track','shared_lane','designated','crossing','share_busway') OR
--         highway = 'track' AND bicycle IN ('designated','yes') OR
--         highway = 'service' AND (bicycle = 'designated' or motor_vehicle ='no') OR
--         highway = 'path' AND bicycle IN ('designated','yes') OR
--         --cyclestreet = 'yes' OR
--         bicycle_road = 'yes'
-- ;

-- UPDATE TABLE osm_edges_simplified
--     SET COLUMN cycling_infrastructure = true
--     WHERE         
--         highway = 'cycleway' OR
--         highway = 'living_street' OR
--         cycleway IN ('lane','track','opposite_lane','opposite_track','shared_lane','designated','crossing','share_busway') OR
--         cycleway_left IN ('lane','track','opposite_lane','opposite_track','shared_lane','designated','crossing','share_busway') OR 
--         cycleway_right IN ('lane','track','opposite_lane','opposite_track','shared_lane','designated','crossing','share_busway') OR
--         cycleway_both IN ('lane','track','opposite_lane','opposite_track','shared_lane','designated','crossing','share_busway') OR
--         highway = 'track' AND bicycle IN ('designated','yes') OR
--         highway = 'service' AND (bicycle = 'designated' or motor_vehicle ='no') OR
--         highway = 'path' AND bicycle IN ('designated','yes') OR
--         --cyclestreet = 'yes' OR
--         bicycle_road = 'yes'
-- ;

-- ALTER TABLE vm_brudt
--     ADD COLUMN cycling_infrastructure VARCHAR DEFAULT NULL
-- ;
-- UPDATE TABLE vm_brudt
--     SET COLUMN cycling_infrastructure = true
--     WHERE vejklasse IN ('Cykelbane langs vej', 'Cykelsti langs vej')
-- ;

-- DROP TABLE IF EXISTS geodk_bike_simple;
-- CREATE TABLE geodk_bike_simple AS
--     SELECT * FROM vm_brudt_simple WHERE vejklasse IN ('Cykelbane langs vej', 'Cykelsti langs vej')
-- ;

-- DROP TABLE IF EXISTS osm_bike;
-- CREATE TABLE osm_bike AS
--     SELECT * FROM osm_edges WHERE 
--         highway = 'cycleway' OR
--         highway = 'living_street' OR
--         cycleway IN ('lane','track','opposite_lane','opposite_track','shared_lane','designated','crossing','share_busway') OR
--         cycleway_left IN ('lane','track','opposite_lane','opposite_track','shared_lane','designated','crossing','share_busway') OR 
--         cycleway_right IN ('lane','track','opposite_lane','opposite_track','shared_lane','designated','crossing','share_busway') OR
--         cycleway_both IN ('lane','track','opposite_lane','opposite_track','shared_lane','designated','crossing','share_busway') OR
--         highway = 'track' AND bicycle IN ('designated','yes') OR
--         highway = 'service' AND (bicycle = 'designated' or motor_vehicle ='no') OR
--         highway = 'path' AND bicycle IN ('designated','yes') OR
--         --cyclestreet = 'yes' OR
--         bicycle_road = 'yes'
-- ;

-- DROP TABLE IF EXISTS osm_no_bike;
-- CREATE TABLE osm_no_bike AS
--     SELECT * FROM osm_edges WHERE NOT EXISTS (
--         SELECT * FROM osm_bike where osm_bike.osmid = osm_edges.osmid
-- );

-- DELETE FROM osm_no_bike WHERE bicycle = 'use_sidepath';
