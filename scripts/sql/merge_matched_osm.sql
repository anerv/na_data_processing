ALTER TABLE osm_edges_simplified
    ADD COLUMN geodk_bike VARCHAR DEFAULT NULL
;

ALTER TABLE osm_edges_simplified
    ADD COLUMN cycling_infra_new VARCHAR DEFAULT 'no'
;

UPDATE osm_edges_simplified SET geodk_bike = om.vejklasse
	FROM osm_matches_roadclass om WHERE osm_edges_simplified.edge_id = om.edge_id
;

UPDATE osm_edges_simplified
    SET cycling_infra_new = 'yes'
        WHERE geodk_bike IN ('Cykelsti langs vej', 'Cykelbane langs vej') 
        OR cycling_infrastructure = 'yes'
;   

UPDATE osm_edges_simplified 
    SET cycling_infra_new = 'no'
        WHERE bicycle = 'no' 
;

-- UPDATE osm_edges_simplified 
--     SET cycling_infra_new = 'no'
--         WHERE highway = 'service' -- Service connectors tends to be misclassified
-- ;
