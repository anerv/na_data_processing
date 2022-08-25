-- IDENTIFY INTERSECTION NODES
CREATE TABLE node_occurences AS
SELECT u AS osmid FROM osm_edges_simplified as osmid
UNION ALL 
SELECT v FROM osm_edges_simplified;

CREATE VIEW node_degrees AS SELECT osmid, COUNT(*) FROM node_occurences GROUP BY osmid;

CREATE TABLE intersections AS SELECT * FROM node_degrees WHERE count > 2;

ALTER TABLE intersections ADD COLUMN geometry geometry(Point,25832);

UPDATE intersections i SET geometry = o.geometry FROM osm_nodes_simplified o WHERE i.osmid = o.osmid;


-- CLASSIFY INTERSECTIONS
ALTER TABLE intersection_tags ADD COLUMN inter_type VARCHAR DEFAULT NULL;

UPDATE intersection_tags SET inter_type = 'unregulated' WHERE 
    (highway NOT IN ('traffic_signals') OR highway IS NULL) AND (crossing IN ('uncontrolled','unmarked') OR crossing IS NULL)
    OR (highway NOT IN ('traffic_signals') OR highway IS NULL) AND (crossing NOT IN ('zebra','marked','controlled','traffic_signals') OR crossing IS NULL)
;

UPDATE intersection_tags SET inter_type = 'marked' WHERE 
    crossing IN ('marked','zebra','island') OR
    'crossing:island' IN ('yes');
    --OR flashing_lights IN ('yes','sensor','button','always');

UPDATE intersection_tags SET inter_type = 'regulated' 
    WHERE crossing = 'traffic_signals' OR highway = 'traffic_signals';


ALTER TABLE intersections ADD COLUMN inter_type VARCHAR DEFAULT NULL;
UPDATE intersections i SET inter_type = it.inter_type FROM intersection_tags it WHERE i.osmid = it.id;


CREATE MATERIALIZED VIEW unmatched_inter_tags AS 
    SELECT * FROM intersection_tags it WHERE NOT EXISTS
    (
        SELECT FROM intersections i WHERE i.osmid = it.id
    )
;

CREATE INDEX un_geom_idx ON unmatched_inter_tags USING GIST (geometry);
CREATE INDEX inter_geom_idx ON intersections USING GIST (geometry);


CREATE VIEW matched_intersections AS
    (SELECT un.id, i.osmid, un.inter_type, un.geometry FROM unmatched_inter_tags AS un
        CROSS JOIN LATERAL ( SELECT osmid FROM intersections
            ORDER BY geometry <-> un.geometry LIMIT  1) AS i
    )
;

UPDATE intersections i SET inter_type = mi.inter_type FROM matched_intersections mi WHERE i.osmid = mi.osmid;

DROP VIEW node_degrees;
DROP VIEW matched_intersections;
DROP MATERIALIZED VIEW unmatched_inter_tags;