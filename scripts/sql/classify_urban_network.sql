ALTER TABLE osm_edges_simplified 
    ADD COLUMN urban_code INT DEFAULT NULL,
    ADD COLUMN urban VARCHAR DEFAULT NULL
;


CREATE VIEW edges_urban AS (
    SELECT edges.edge_id, edges.u, edges.v,  u.urban_code urban1, v.urban_code urban2 
    FROM osm_edges_simplified edges 
        JOIN urban_nodes u ON edges.u = u.osmid 
        JOIN urban_nodes v ON edges.v = v.osmid
    )
;


CREATE VIEW edges_urban_code AS 
    (SELECT edge_id, GREATEST(urban1, urban2) AS urban_code FROM edges_urban)
;

UPDATE osm_edges_simplified e 
    SET urban_code = u.urban_code 
    FROM edges_urban_code u WHERE e.edge_id = u.edge_id
;

UPDATE osm_edges_simplified SET urban = 'rural' WHERE urban_code IN (11,12);
UPDATE osm_edges_simplified SET urban = 'semi-rural' WHERE urban_code = 13;
UPDATE osm_edges_simplified SET urban = 'sub-semi-urban' WHERE urban_code IN (21,22);
UPDATE osm_edges_simplified SET urban = 'urban' WHERE urban_code IN (23,30);

DROP VIEW edges_urban_code;
DROP VIEW edges_urban;