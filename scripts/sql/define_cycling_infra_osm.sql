ALTER TABLE osm_edges_simplified ADD COLUMN cycling_infrastructure VARCHAR DEFAULT 'no';

UPDATE osm_edges_simplified SET cycling_infrastructure = 'yes' WHERE
    highway IN ('cycleway', 'living_street') OR
    cycleway IN ('lane','track','opposite_lane','opposite_track','shared_lane','designated','crossing','share_busway','shared_lane','shared_lane;shared') OR
    cycleway_left IN ('lane','track','opposite_lane','opposite_track','shared_lane','designated','crossing','share_busway','shared_lane','shared_lane;shared') OR
    cycleway_right IN ('lane','track','opposite_lane','opposite_track','shared_lane','designated','crossing','share_busway','shared_lane','shared_lane;shared') OR
    cycleway_both IN ('lane','track','opposite_lane','opposite_track','shared_lane','designated','crossing','share_busway','shared_lane','shared_lane;shared') OR
    bicycle_road = 'yes' OR
    cyclestreet = 'yes' OR
    (highway = 'track' AND bicycle IN ('designated','yes')) OR
    (highway = 'path' AND bicycle IN ('designated','yes'))
;

UPDATE osm_edges_simplified SET cycling_infrastructure = 'no' WHERE bicycle IN ('no','dismount');

-- # ox_edges['cycling_infrastructure'] = 'no'

-- # queries = ["highway == 'cycleway'",
-- #         "highway == 'living_street'",
-- #         "cycleway in ['lane','track','opposite_lane','opposite_track','shared_lane','designated','crossing','share_busway','shared_lane','shared_lane;shared']",
-- #         "cycleway_left in ['lane','track','opposite_lane','opposite_track','shared_lane','designated','crossing','share_busway','shared_lane']",
-- #         "cycleway_right in ['lane','track','opposite_lane','opposite_track','shared_lane','designated','crossing','share_busway','shared_lane']",
-- #         "cycleway_both in ['lane','track','opposite_lane','opposite_track','shared_lane','designated','crossing','share_busway','shared_lane']",
-- #         "bicycle_road == 'yes'",
-- #         "cyclestreet == 'yes'",
-- #         "highway == 'track' & bicycle in ['designated','yes']",
-- #         "highway == 'path' & bicycle in ['designated','yes']" 
-- #         ]

-- # for q in queries:
-- #     ox_filtered = ox_edges.query(q)

-- #     ox_edges.loc[ox_filtered.index, 'cycling_infrastructure'] = 'yes'

-- # ox_edges.loc[ox_edges.index[ox_edges['bicycle'].isin(['no','dismount'])],'cycling_infrastructure'] = 'no'
