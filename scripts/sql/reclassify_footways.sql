# MISSING: Write query that finds all footways matched to GEODK
- if path (e.g. connected edges) are less than 50 meters, set to null


CREATE VIEW matched footways AS
    SELECT * FROM osm_edges_simplified 
        WHERE geodk_bike IS NOT NULL AND highway = 'footway'
;



CREATE  TABLE test_lines (id serial, geom geometry);

INSERT INTO test_lines (geom)
VALUES
('LINESTRING (0 0, 1 1)'),
('LINESTRING (2 2, 1 1)'),
('LINESTRING (7 3, 0 0)'),
('LINESTRING (2 4, 2 3)'),
('LINESTRING (3 8, 1 5)'),
('LINESTRING (1 5, 2 5)'),
('LINESTRING (7 3, 0 7)');

WITH endpoints AS (SELECT ST_Collect(ST_StartPoint(geom), ST_EndPoint(geom)) AS geom FROM test_lines),
     clusters  AS (SELECT unnest(ST_ClusterWithin(geom, 1e-8)) AS geom FROM endpoints),
     clusters_with_ids AS (SELECT row_number() OVER () AS cid, ST_CollectionHomogenize(geom) AS geom FROM clusters)
SELECT ST_Collect(test_lines.geom) AS geom
FROM test_lines
LEFT JOIN clusters_with_ids ON ST_Intersects(test_lines.geom, clusters_with_ids.geom)
GROUP BY cid;


-- WITH data(geom) AS SELECT geometry FROM footways,
-- merged AS (SELECT (ST_Dump( ST_LineMerge( ST_Collect(geom) ) )).geom FROM data
-- )
-- SELECT row_number() OVER () AS cid, geom FROM merged;

WITH data(geom) AS SELECT geometry FROM footways,
merged AS (SELECT (ST_Dump( ST_LineMerge( ST_Collect(geom) ) )).geom FROM data
)
CREATE VIEW merged_footways AS SELECT row_number() OVER () AS cid, geom FROM merged;

-- Compute length of merged footways

-- Select those with length less than XXX
-- Find a way of setting those as not matches in org data


