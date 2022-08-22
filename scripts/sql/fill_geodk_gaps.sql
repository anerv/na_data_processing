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
    SELECT * FROM osm_edges_simplified 
    WHERE (u IN (SELECT u FROM gaps_nodes) OR v IN (SELECT u FROM gaps_nodes))
    AND name IN (SELECT name FROM gaps) AND geodk_bike IS NOT NULL
;

CREATE TABLE gaps_joined AS 
    SELECT array_agg(g.edge_id) AS edge_ids, 
    gaps.geometry AS geometry, 
    UNNEST(ARRAY_AGG(DISTINCT(gaps.edge_id))) AS gaps_id, 
    UNNEST(ARRAY_AGG(DISTINCT(g.geodk_bike))) AS roadclass 
FROM gaps_connectors g, gaps 
    WHERE ST_Touches(g.geometry, gaps.geometry) GROUP BY gaps.geometry
;

DELETE FROM gaps_joined WHERE gaps_id IS NULL;

UPDATE osm_edges_simplified 
SET geodk_bike = g.roadclass FROM gaps_joined g WHERE osm_edges_simplified.edge_id = g.gaps_id;


-- CREATE TABLE gaps_and_links AS SELECT * FROM gaps
-- UNION
-- SELECT * FROM gaps_connectors
-- ;

-- CREATE TABLE merged AS  
--     WITH data AS (SELECT edge_id, geodk_bike, geometry FROM gaps_and_links)
--         SELECT (ST_Dump( ST_LineMerge( ST_Collect(geometry) ) )).geom FROM data
-- ;
