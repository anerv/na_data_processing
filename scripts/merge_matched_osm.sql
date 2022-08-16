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
