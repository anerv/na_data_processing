
###Thoughts on using osmnx, pandana etc. for initial data processing
osmnx and pandana create the same number of edges (indicating same structure for network)
- but pandana seems to be a simpler df structure.
- osmnx needs the multiindex
- seems to be simpler to create pandana edge list from osmnx than vice versa
- workflow could be to load osmnx data to postgis database - do processing - load back to geodataframe
- and then convert to which graph type I need
