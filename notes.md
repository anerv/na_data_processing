
###Thoughts on using osmnx, pandana etc. for initial data processing
osmnx and pandana create the same number of edges (indicating same structure for network)
- but pandana seems to be a simpler df structure.
- osmnx needs the multiindex
- seems to be simpler to create pandana edge list from osmnx than vice versa
- workflow could be to load osmnx data to postgis database - do processing - load back to geodataframe
- and then convert to which graph type I need



type: way and
(sidewalk: left:bicycle=yes) or
(cycleway: left=shared lane) or
(cyclestreet=yes) or
(cycleway: left=shared busway) or
(cycleway:right=shared busway) or
(cycleway=shared busway) or
(cvclewav=opposite lane) or
(highway=bridleway and bicycle=no) or
(highway=track and bicycle=designated and motor vehicle=no) or
(bicvcle=use sidepath) or
(cycleway=opposite and oneway: bicycle=no) or
(sidewalk: right:bicycle=yes) or
(cycleway:right=shared_lane)
or
(cyclewav: left=track) or
(cycleway: right=track) or
(highway=track and bicycle=designated and motor vehicle=no) or
(highway=path and bicycle=yes) or
(highway=path and (bicycle=designated or bicycle=official))
or
(highway=service and (bicycle=designated or motor vehicle=no)) or
(highway=pedestrian and (bicycle=yes or bicycle=official)) or
(highwav=footway and (bicycle=ves or bicycle=official)) or
(hiehway=cycleway) or (cycleway in (lane, opposite lane,
shared busway, track, opposite track)) or
(cvclewav: left in (lane, shared _busway)) or
(cycleway:right in (lane, shared busway)) or
(cycleway: both=lane) or
(bicycle_road=yes and (motor vehicle=no or bicycle=designated)) or
(cvclestreet=ves)

