-- Script if an edge is:
-- a street
-- connected to two edges where each of them is Geodk not NULL
-- less than XX meters long
    -- set to cycling infra yes (take value from GeoDK edges)

-- Fix gaps in newly classified cycling_infrastructure
CREATE VIEW potential_gaps AS
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
            AND cycling_infra_new != 'yes'
;

-- CREATE column of nodes where geodk is not null
CREATE VIEW geodk_nodes AS 
    SELECT u FROM osm_edges_simplified WHERE geodk_bike IS NOT NULL 
    UNION
    SELECT v FROM osm_edges_simplified WHERE geodk_bike IS NOT NULL
;

CREATE VIEW gaps AS
    SELECT * FROM potential_gaps 
        WHERE u IN (SELECT u FROM geodk_nodes) AND ST_Length(geometry) < 10
;

UPDATE osm_edges_simplified SET cycling_infra_new = 'yes' WHERE 
JOIN osm_edges_simplified ON gaps 

UPDATE osm_edges_simplified SET cycling_infra_new = 'yes'
	FROM gaps WHERE osm_edges_simplified.edge_id = gaps.edge_id
;

-- Repeat with smaller tolerance between geodk and cycling infra?


------

CREATE VIEW potential_gaps_test AS
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
            AND cycling_infrastructure != 'yes'
;

-- CREATE column of nodes where geodk is not null
CREATE VIEW geodk_nodes_test AS 
    SELECT u FROM osm_edges_simplified WHERE geodk_bike IS NOT NULL 
    UNION
    SELECT v FROM osm_edges_simplified WHERE geodk_bike IS NOT NULL
;

CREATE VIEW gaps_test AS
    SELECT * FROM potential_gaps_test 
        WHERE u IN (SELECT u FROM geodk_nodes_test) AND ST_Length(geometry) < 10
;


UPDATE osm_edges_simplified SET cycling_infra_new = 'yes'
	FROM gaps WHERE osm_edges_simplified.edge_id = gaps.edge_id
;