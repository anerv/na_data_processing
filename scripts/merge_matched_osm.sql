ALTER TABLE osm_edges_simplified
    ADD COLUMN geodk_bike VARCHAR DEFAULT NULL
;

ALTER TABLE osm_edges_simplified
    ADD COLUMN cycling_infra_new VARCHAR DEFAULT NULL
;

UPDATE osm_edges_simplified SET geodk_bike = om.vejklasse
	FROM osm_matches om WHERE osm_edges_simplified.edge_id = om.edge_id
;

UPDATE osm_edges_simplified 
    SET geodk_bike = NULL
        WHERE highway = 'service' -- Service connectors tends to be misclassified
;

UPDATE osm_edges_simplified
    SET cycling_infra_new = 'yes'
        WHERE geodk_bike IN ('Cykelsti langs vej', 'Cykelbane langs vej') 
        OR cycling_infrastructure = 'yes'
;   

-- Script if an edge is:
-- a street
-- connected to two edges where each of them is Geodk not NULL
-- less than XX meters long
    -- set to cycling infra yes (take value from GeoDK edges)