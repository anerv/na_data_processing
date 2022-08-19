ALTER TABLE osm_edges_simplified
    ADD COLUMN cycling_surface_as VARCHAR,
    ADD COLUMN lit_as VARCHAR,
    ADD COLUMN speed_as VARCHAR 
;

-- SURFACE
ALTER TABLE osm_matches_surface
    ADD COLUMN surface VARCHAR;

UPDATE osm_matches_surface SET surface = 'paved' WHERE overflade = 'Befæstet';
UPDATE osm_matches_surface SET surface = 'unpaved' WHERE overflade = 'Ubefæstet';
UPDATE osm_matches_surface SET surface = 'unknown' WHERE overflade = 'Ukendt';
UPDATE osm_matches_surface SET surface = 'paved' WHERE surface IS NULL; -- Catch edges with both befæstet/ubefæstet due to simplification 

UPDATE osm_edges_simplified SET cycling_surface = om.overflade
    FROM osm_matches_surface om WHERE osm_edges_simplified.edge_id = om.edge_id
    AND cycling_surface IS NULL;
;

UPDATE osm_edges_simplified SET surface_as = surface;
UPDATE osm_edges_simplified SET surface_as = cycling_surface;

UPDATE osm_edges_simplified 
    SET surface_as = 'paved' 
        WHERE geodk_bike IS NOT NULL 
        AND surface_as IS NULL
; 

    
UPDATE osm_edges_simplified 
    SET surface_as = 'paved' 
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
            'motorway',
            'motorway_link',
            'service') 
            AND surface_as IS NULL
;

UPDATE osm_edges_simplified 
    SET surface_as = 'paved' 
        WHERE along_street = true AND surface IS NULL;


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
            'residential',
            'motorway',
            'motorway_link',
            'service')  
        AND lit_as IS NULL
;

UPDATE osm_edges_simplified 
    SET lit_as = 'yes' 
        WHERE along_street = true 
        AND highway = 'cycleway' 
        AND urban_area = 'yes'
        AND lit_as IS NULL;
; -- is this safe to assume??

--UPDATE osm_edges_simplified SET lit_as = 'yes' WHERE along_street = true IF -- intersects with urban area??

-- SPEED 
UPDATE osm_edges_simplified SET speed_as = speed;
UPDATE osm_edges_simplified SET speed_as
    CASE

        ('trunk',
        'trunk_link',
        'tertiary',
        'tertiary_link',
        'secondary',
        'secondary_link',
        'living_street',
        'primary',
        'primary_link',
        'residential',
        'motorway',
        'motorway_link',
        'service') 