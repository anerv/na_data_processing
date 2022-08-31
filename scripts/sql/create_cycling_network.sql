
CREATE TABLE cycling_edges AS SELECT * FROM osm_edges_simplified;

DELETE FROM cycling_edges WHERE 
    cycling_infra_new != 'yes' AND
    (bicycle IN ('no','dismount','private','use_sidepath') OR
    access IN ('private','restricted','customers','no') OR
    highway IN ('motorway','motorway_link') --OR
    --(highway = 'footway' AND ( (bicycle NOT IN ('allowed','ok','designated','permissive','yes','destination')) OR bicycle IS NULL) ) OR
    --(highway = 'pedestrian' AND ((bicycle NOT IN ('allowed','ok','designated','permissive','yes','destination')) OR bicycle IS NULL) )
    )
;

CREATE TABLE cycling_nodes AS 
	SELECT * FROM osm_nodes_simplified 
    WHERE osmid IN
        (SELECT u FROM cycling_edges
        UNION
        SELECT v FROM cycling_edges)
;


