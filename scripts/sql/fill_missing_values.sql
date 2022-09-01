ALTER TABLE osm_edges_simplified
    ADD COLUMN cycling_surface_as VARCHAR DEFAULT NULL, -- 'as' = 'assumed'
    ADD COLUMN lit_as VARCHAR DEFAULT NULL,
    ADD COLUMN speed_as VARCHAR DEFAULT NULL
;


-- Limiting number of road segments with road type 'unknown'
CREATE VIEW unknown_roadtype AS 
    (SELECT name, osmid, highway, geometry FROM osm_edges_simplified WHERE highway = 'unclassified')
;

CREATE VIEW known_roadtype AS 
    (SELECT name, osmid, highway, geometry FROM osm_edges_simplified 
        WHERE highway != 'unclassified' AND highway != 'cycleway');

UPDATE unknown_roadtype uk SET highway = kr.highway FROM known_roadtype kr 
    WHERE ST_Touches(uk.geometry, kr.geometry) AND uk.name = kr.name;

-- UPDATE unknown_roadtype uk SET highway = kr.highway FROM known_roadtype kr 
--     WHERE uk.name = kr.name AND uk.highway = 'unclassified';

DROP VIEW unknown_roadtype;
DROP VIEW known_roadtype;


-- SURFACE
ALTER TABLE osm_matches_surface
    ADD COLUMN surface VARCHAR
;
    
UPDATE osm_matches_surface SET surface = 'paved' WHERE overflade = 'Befæstet';
UPDATE osm_matches_surface SET surface = 'unpaved' WHERE overflade = 'Ubefæstet';
UPDATE osm_matches_surface SET surface = 'unknown' WHERE overflade = 'Ukendt';
UPDATE osm_matches_surface SET surface = 'paved' WHERE surface IS NULL; -- Catch edges with both befæstet/ubefæstet due to simplification 

-- Surface from GeoDK is not assumed
UPDATE osm_edges_simplified SET cycleway_surface = om.surface
    FROM osm_matches_surface om WHERE osm_edges_simplified.edge_id = om.edge_id
    AND cycleway_surface IS NULL;
;

UPDATE osm_edges_simplified SET cycling_surface_as = surface WHERE surface != 'unknown' AND cycling_infra_new = 'yes';
UPDATE osm_edges_simplified SET cycling_surface_as = cycleway_surface;

-- Cycling surface is assumed paved if along a car street    
UPDATE osm_edges_simplified 
    SET cycling_surface_as = 'paved' 
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
            --'service',
            'motorway',
            'motorway_link'
            ) 
            AND cycling_surface_as IS NULL
            AND cycling_allowed = 'yes';
;

UPDATE osm_edges_simplified 
    SET cycling_surface_as = 'paved' 
        WHERE along_street = 'true' AND surface IS NULL AND cycling_infra_new = 'yes'
;

-- UPDATE BASED ON URBAN AREAS
-- LIT
UPDATE osm_edges_simplified SET lit_as = lit;
UPDATE osm_edges_simplified 
    SET lit_as = 'yes' 
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
            --'residential',
            'motorway',
            'motorway_link')  
        AND lit_as IS NULL
        AND urban_area = 'yes'
;

UPDATE osm_edges_simplified 
    SET lit_as = 'yes' 
        WHERE along_street = 'true' 
        AND highway = 'cycleway' 
        AND urban_area = 'yes'
        AND lit_as IS NULL
;

 -- is this safe to assume??

--UPDATE osm_edges_simplified SET lit_as = 'yes' WHERE along_street = 'true' IF -- intersects with urban area??

-- SPEED 
-- UPDATE osm_edges_simplified SET speed_as = speed;
-- UPDATE osm_edges_simplified WHERE speed_as IS NULL
--     SET speed_as
--         CASE
--             WHEN highway IN ('motorway','motorway_link',) THEN 130
--             WHEN highway IN ('residential') THEN 
--             WHEN highway IN ('trunk','trunk_link') THEN
--             WHEN highway IN ('living_street','bicycle_street') THEN
--         END
-- ;


--         ('trunk', 'trunk_link',
--         'tertiary',
--         'tertiary_link',
--         'secondary',
--         'secondary_link',
--         'living_street',
--         'primary',
--         'primary_link',
--         'residential',
  
--         'service') 