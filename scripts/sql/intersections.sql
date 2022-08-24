-- IDENTIFY INTERSECTION NODES
CREATE TABLE node_occurences AS
SELECT u FROM osm_edges_simplified
UNION ALL 
SELECT v FROM osm_edges_simplified;

CREATE VIEW node_degrees AS SELECT u, COUNT(*) FROM node_occurences GROUP BY u;

CREATE TABLE intersections AS SELECT * FROM node_degrees WHERE count > 2;

ALTER TABLE intersections ADD COLUMN geometry geometry(Point,25832);

UPDATE intersections i SET geometry = o.geometry FROM osm_nodes_simplified o WHERE i.u = o.osmid;


-- CLASSIFY INTERSECTIONS
ALTER TABLE intersection_tags ADD COLUMN inter_type VARCHAR DEFAULT NULL;

UPDATE intersection_tags SET inter_type = 'unregulated' 
    WHERE highway NOT IN ('traffic_signals','crossing') 
    AND crossing IN ('uncontrolled','unmarked');

UPDATE intersection_tags SET inter_type = 'marked' WHERE 
    crossing IN ('marked','zebra','island') OR

UPDATE intersection_tags SET inter_type = 'regulated' 
    WHERE crossing = 'traffic_signals' OR highway = 'traffic_signals';



-- JOIN TO OSM graph intersections
-- SET OTHERS TO unregulated or unknown?

-- regulated intersections
crossing=traffic_signals
highway = traffic_signals

-- are there any nodes just tagged as highway = crossing??

-- marked intersections
crossing = uncontrolled
crossing = marked
crossing = zebra
crossing = island
crossing:island = 'yes'
flashing_lights in ('yes','sensor','button','always',)
and highway != traffic_signals

-- unregulated intersections
crossing=unmarked
-- and all the remaining?
