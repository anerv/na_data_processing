-- Fix gaps in newly classified cycling_infrastructure
CREATE TABLE potential_gaps AS
    SELECT * FROM osm_edges_simplified 
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
            'track',
            'path',
            'motorway',
            'motorway_link',
            'unclassified') 
            AND (cycling_infra_new != 'yes' OR cycling_infra_new IS NULL)
;

CREATE TABLE matched_nodes AS 
    SELECT u FROM osm_edges_simplified WHERE geodk_bike IS NOT NULL 
    UNION
    SELECT v FROM osm_edges_simplified WHERE geodk_bike IS NOT NULL
;

CREATE VIEW matched_names AS
    SELECT name, vejklasse FROM osm_edges_simplified WHERE geodk_bike IS NOT NULL;

CREATE TABLE gaps AS
    SELECT * FROM potential_gaps 
        WHERE u IN (SELECT u FROM matched_nodes) 
        AND v IN (SELECT u FROM matched_nodes) 
		AND name in (SELECT NAME FROM matched_names)
        AND ST_Length(geometry) < 20
;

DELETE FROM gaps WHERE bicycle = 'no';

CREATE VIEW gaps_nodes AS 
    SELECT u FROM gaps 
    UNION
    SELECT v FROM gaps
;

CREATE TABLE gaps_connectors AS
SELECT * FROM osm_edges_simplified WHERE 
(u IN (SELECT u FROM gaps_nodes) OR v IN (SELECT u FROM gaps_nodes))
AND name IN (SELECT name FROM gaps) AND geodk_bike IS NOT NULL;


CREATE TABLE gaps_links AS SELECT * FROM gaps
UNION
SELECT * FROM gaps_connectors;


--DROP VIEW endpoints;
--CREATE VIEW endpoints AS (SELECT ST_Collect(ST_StartPoint(geometry), ST_EndPoint(geometry)) AS geom, edge_id FROM gaps_links);
--DROP VIEW clusters;
-- CREATE VIEW clusters  AS (SELECT unnest(ST_ClusterWithin(geom, 1e-8)) AS geom FROM endpoints);

--DROP VIEW clusters_with_ids;
--CREATE VIEW clusters_with_ids AS (SELECT row_number() OVER () AS cid, ST_CollectionHomogenize(geom) AS geom FROM clusters);
---- SELECT ST_Collect(gaps_links.geometry) AS geom
-- FROM gaps_links
-- LEFT JOIN clusters_with_ids ON ST_Intersects(gaps_links.geometry, clusters_with_ids.geom)
-- GROUP BY cid;

--SELECT * FROM clusters_with_ids;

SELECT * FROM endpoints;



WITH endpoints AS (SELECT ST_Collect(ST_StartPoint(geom), ST_EndPoint(geom)) AS geom FROM gaps_links),
     clusters  AS (SELECT unnest(ST_ClusterWithin(geom, 1e-8)) AS geom FROM endpoints),
     clusters_with_ids AS (SELECT row_number() OVER () AS cid, ST_CollectionHomogenize(geom) AS geom FROM clusters)
SELECT ST_Collect(test_lines.geom) AS geom
FROM test_lines
LEFT JOIN clusters_with_ids ON ST_Intersects(test_lines.geom, clusters_with_ids.geom)


--GROUP BY cid;




WITH data(geom) AS SELECT geometry FROM gaps_links,
merged AS (SELECT (ST_Dump( ST_LineMerge( ST_Collect(geom) ) )).geom FROM data
)
CREATE VIEW merged_gaps AS SELECT row_number() OVER () AS cid, geom FROM merged;

-- SELECT all edges that share a node with gaps
-- 




UPDATE osm_edges_simplified SET cycling_infra_new = 'yes' WHERE 
JOIN osm_edges_simplified ON gaps 

UPDATE osm_edges_simplified SET cycling_infra_new = 'yes'
	FROM gaps WHERE osm_edges_simplified.edge_id = gaps.edge_id
;

-- Repeat with smaller tolerance between geodk and cycling infra?

