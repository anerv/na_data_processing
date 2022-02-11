
###Thoughts on using osmnx, pandana etc. for initial data processing
osmnx and pandana create the same number of edges (indicating same structure for network)
- but pandana seems to be a simpler df structure.
- osmnx needs the multiindex
- seems to be simpler to create pandana edge list from osmnx than vice versa
- workflow could be to load osmnx data to postgis database - do processing - load back to geodataframe
- and then convert to which graph type I need


-- bicycle = 'use_sidepath' ??
-- highway = 'bridleway'?
-- include paths?


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




WHERE highway = 'cycleway'
OR cycleway ILIKE '%designated%' 
OR cycleway ILIKE '%crossing%'
OR cycleway ILIKE '%lane%' 
OR cycleway ILIKE '%opposite_%' 
OR cycleway ILIKE '%track%' 
OR cycleway ILIKE '%yes%'
OR "cycleway:left" ILIKE '%lane%'
OR "cycleway:left" ILIKE '%opposite_%'
OR "cycleway:left" ILIKE '%track%'
OR "cycleway:right" ILIKE '%lane%'
OR "cycleway:right" ILIKE '%opposite_%'
OR "cycleway:right" ILIKE '%track%'
OR "cycleway:both" ILIKE '%lane%'
OR "cycleway:both" ILIKE '%opposite_%'
OR "cycleway:both" ILIKE '%track%';

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



{'car30': ('network tvne': 'drive'
"custom filter': ' ["maxspeed"~"^30$|^20$|^15$|^10$|^5$|^20 mph|^15 mph|^10 mph| ^5 mph"]',
"export': True,
"carat network tyne" arive".
"custom filter: None,
"export': True,
'retain all': False}
"hike cvclewavtrack: network toe : b1ke,
"custom filter': ' ["cycleway"~"track")',
export': False,
'retain all': True},
"bike highwaycycleway': ('network _type : bike' .
'custom_filter': ["highway"~"cycleway"]',
*export': False, 'retain all': True}.
"hike desionatedpath': «'network type: all',
'custom_filter': ' ["highway"~"path"] ["bicycle"»"designated"]', 'export': False, 'retain_all': T
"bike cvclewavrighttrack': (*network type': 'bike".
*custom filter': ["cvclewav: right"-"track"]".
'export': False, 'retain_all': True),
•bike cvclewavlefttrack': «'network type : bike',
'custom _filter': ' [cycleway: left"~"track"]',
*export': False,
retain all': True}.
'bike_cyclestreet': ('network type': 'bike', 'custom filter': ' ["cyclestreet"]',
Toynort.• False.
retain all': True}.
bike _bicycleroad': ('network_type": 'bike',
*custom filter': ' ["bicycle_road"]',
"export': False,
"retain all': True}
'bike