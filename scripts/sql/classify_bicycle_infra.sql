-- For all edges where cycling is allowed, I want to know
-- if there is cycling infra (yes, no) DONE
-- if the cycling infra is protected (yes, no)
-- if you are cycling in mixed traffic (for all edges)
-- if cycling infra is along a street
-- if there are lights (yes, no)
-- the speed limit
-- what type of intersections we have

ALTER TABLE osm_edges_simplified
    ADD COLUMN cycling_allowed VARCHAR DEFAULT NULL,
    ADD COLUMN protected VARCHAR DEFAULT NULL,
    ADD COLUMN car_traffic VARCHAR DEFAULT NULL,
    ADD COLUMN bike_separated VARCHAR DEFAULT NULL,
    ADD COLUMN along_street VARCHAR DEFAULT NULL
;

UPDATE osm_edges_simplified SET maxspeed = NULL WHERE maxspeed = 'unknown';
UPDATE osm_edges_simplified SET cycleway = NULL WHERE cycleway = 'unknown';
UPDATE osm_edges_simplified SET cycleway_both = NULL WHERE cycleway_both = 'unknown';
UPDATE osm_edges_simplified SET cycleway_left = NULL WHERE cycleway_left = 'unknown';
UPDATE osm_edges_simplified SET cycleway_right = NULL WHERE cycleway_right = 'unknown';
UPDATE osm_edges_simplified SET bicycle_road = NULL WHERE bicycle_road = 'unknown';
UPDATE osm_edges_simplified SET surface = NULL WHERE surface = 'unknown';
UPDATE osm_edges_simplified SET lit = NULL WHERE lit = 'unknown';


-- Updating value in column car_traffic
UPDATE osm_edges_simplified SET car_traffic = 'yes' 
    WHERE highway IN (
            'trunk',
            'trunk_link',
            'tertiary',
            'tertiary_link',
            'secondary',
            'secondary_link',
            'living_street',
            'primary',
            'primary_link',
            'residential',
            'motorway',
            'motorway_link',
            'service') 
        OR highway = 'unclassified' AND ('name' IS NOT NULL AND (access IS NULL OR access NOT IN ('no', 'restricted')) AND motorcar != 'no' AND motor_vehicle != 'no')
        OR highway = 'unclassified' AND ((maxspeed::integer > 15) AND (motorcar != 'no' OR motorcar is NULL) AND (motor_vehicle != 'no' OR motor_vehicle IS NULL));


UPDATE osm_edges_simplified
    SET cycling_allowed = 'yes' 
        WHERE bicycle IN ('yes','permissive', 'ok', 'allowed', 'designated')
        OR cycling_infra_new = 'yes'
        OR (highway IN (
            'trunk',
            'trunk_link',
            'tertiary',
            'tertiary_link',
            'secondary',
            'secondary_link',
            'living_street',
            'primary',
            'primary_link',
            'residential',
            'service',
            'unclassified',
            'path',
            'track')
            AND (access IS NULL OR access NOT IN ('no', 'restricted'))
                AND (bicycle IS NULl OR bicycle NOT IN ('no','dismount','use_sidepath')) )
;

UPDATE osm_edges_simplified
    SET cycling_allowed = 'no'
        WHERE bicycle IN ('no', 'dismount', 'use_sidepath')
        OR (highway IN ('motorway','motorway_link') AND cycling_infra_new = 'no')
;

-- -- Segments where pedestrians are allowed
-- UPDATE osm_edges_simplified 
--     SET pedestrian_allowed = 'yes' 
--         WHERE highway in ('pedestrian', 'path', 'footway', 'steps')
--         OR foot IN ('yes', 'designated', 'permissive', 'official', 'destination')
--         OR sidewalk IN ('both', 'left', 'right')
-- ;

UPDATE osm_edges_simplified 
    SET protected = 'true'
        WHERE 
        cycling_infra_new = 'yes' AND
        (geodk_bike = 'Cykelsti langs vej' OR
        highway IN ('cycleway','track','path') OR
        cycleway IN ('track','opposite_track') OR
        cycleway_left IN ('track','opposite_track') OR
        cycleway_right IN ('track','opposite_track') OR 
        cycleway_both IN ('track','opposite_track'))
;


UPDATE osm_edges_simplified 
    SET protected = 'mixed'
        WHERE
        protected = 'true' AND
        geodk_bike = 'Cykelbane langs vej' OR
        bicycle_road = 'yes' OR
        cycleway IN ('lane','opposite_lane','shared_lane','crossing') OR
        cycleway_left in ('lane','opposite_lane','shared_lane','crossing') OR
        cycleway_right in ('lane','opposite_lane','shared_lane','crossing') OR
        cycleway_both in ('lane','opposite_lane','shared_lane','crossing')
;


UPDATE osm_edges_simplified
    SET protected = 'false'
        WHERE
        protected IS NULL AND 
        cycling_infra_new = 'yes' AND 
        (geodk_bike = 'Cykelbane langs vej' OR
        bicycle_road = 'yes' OR
        highway = 'living_street' OR
        cyclestreet = 'yes' OR
        cycleway IN ('lane','opposite_lane','shared_lane','crossing','shared_lane;shared','share_busway') OR
        cycleway_left in ('lane','opposite_lane','shared_lane','crossing') OR
        cycleway_right in ('lane','opposite_lane','shared_lane','crossing') OR
        cycleway_both in ('lane','opposite_lane','shared_lane','crossing'))
;

UPDATE osm_edges_simplified 
    SET protected = 'unknown' 
        WHERE protected IS NULL AND cycling_infra_new = 'yes'
;

-- Cycling infrastructure separated from car street network - i.e. you are not biking in mixed traffic
-- False if not known to be true
UPDATE osm_edges_simplified
    SET bike_separated = 'false' 
        WHERE cycling_infra_new = 'yes'
;

UPDATE osm_edges_simplified
    SET bike_separated = 'true' 
        WHERE highway IN ('cycleway')
        OR cycleway IN ('lane','track','opposite_lane','opposite_track')
        OR cycleway_left IN ('lane','track','opposite_lane','opposite_track') 
        OR cycleway_right IN ('lane','track','opposite_lane','opposite_track')
        OR cycleway_both IN ('lane','track','opposite_lane','opposite_track')
        OR highway IN ('path','track') AND bicycle IN ('designated','yes')
        OR bicycle = 'designated' and (motor_vehicle = 'no' OR motorcar = 'no')
        OR geodk_bike IN ('Cykelsti langs vej','Cykelbane langs vej')
;

--Determining whether the segment of cycling infrastructure runs along a street or not
-- Along a street with car traffic
UPDATE osm_edges_simplified 
    SET along_street = 'false' 
        WHERE cycling_infra_new = 'yes' AND along_street IS NULL
;

UPDATE osm_edges_simplified 
    SET along_street = 'true' 
        WHERE car_traffic = 'yes' AND cycling_infra_new = 'yes'
;

UPDATE osm_edges_simplified 
    SET along_street = 'true' 
        WHERE geodk_bike IS NOT NULL
;

-- Capturing cycleways digitized as individual ways both still running parallel to a street
CREATE VIEW cycleways AS 
    (SELECT name, highway, cycling_infrastructure, along_street FROM osm_edges_simplified
        WHERE highway = 'cycleway')
;


CREATE VIEW car_roads AS 
    (SELECT name, highway, geometry FROM osm_edges_simplified
        WHERE car_traffic = 'yes') -- should it include service?
;

-- UPDATE cycleways c SET along_street = 'true'
--     FROM car_roads cr WHERE c.name = cr.name
-- ;

CREATE TABLE buffered_car_roads AS 
	(SELECT (ST_Dump(geom)).geom FROM 
        (SELECT ST_Union(ST_Buffer(geometry,30)) AS geom FROM car_roads) cr)
;

CREATE INDEX buffer_geom_idx ON buffered_car_roads USING GIST (geom);

CREATE TABLE intersecting_cycle_roads AS 
(SELECT o.edge_id, o.geometry FROM osm_edges_simplified o, buffered_car_roads br
WHERE o.cycling_infra_new = 'yes' AND ST_Intersects(o.geometry, br.geom));

CREATE TABLE cycle_infra_points AS
(SELECT edge_id, ST_Collect( ARRAY[ST_StartPoint(geometry), ST_Centroid(geometry), ST_EndPoint(geometry)]) AS geometry FROM intersecting_cycle_roads);

CREATE INDEX cycle_points_geom_idx ON cycle_infra_points USING GIST (geometry);

CREATE TABLE cycling_cars AS
(SELECT c.edge_id, c.geometry FROM cycle_infra_points c, buffered_car_roads br
WHERE ST_CoveredBy(c.geometry, br.geom));

UPDATE osm_edges_simplified o SET along_street = 'true' FROM cycling_cars c 
WHERE o.edge_id = c.edge_id; 


DROP VIEW cycleways;
DROP VIEW car_roads;
DROP TABLE buffered_car_roads;
DROP TABLE cycling_cars;
DROP TABLE intersecting_cycle_roads;
DROP TABLE cycle_infra_points;

-- TODO - set along street if completely within a buffer from road of 20 meters



-- EDGES LEADING TO UNREGULATED INTERSECTIONS

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

-- UPDATE osm_edges_simplified
--     SET cycling_infrastructure = 'yes'
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
