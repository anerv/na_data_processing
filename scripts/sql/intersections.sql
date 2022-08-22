-- first fix load osm so node attributes are included!

-- classify all nodes that are intersections -- all of them?


-- 'crossing',,'bollard','flashing_lights


write query that finds all nodes that are not start/end points - mark them as intersections
# Nodes with degrees more than 2?


# Classify intersections as signalled/controlled or not

-- regulated intersections
crossing=traffic_signals
highway = traffic_signals

-- are there any nodes just tagged as highway = crossing??

-- marked intersections
crossing = uncontrolled
crossing = marked
crossing = zebra
crossing = island
crossing:island = 'yes'
flashing_lights in ('yes','sensor','button','always',)
and highway != traffic_signals

-- unregulated intersections
crossing=unmarked
-- and all the remaining?

-- unsafe intersections
-- intersections involving highways above a certain class
-- select all high stress streets (regardless of cycling infra)
-- get their end node - if it is marked as unregulated - bad bad - if just marked - also a bit bad