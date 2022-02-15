
-- Working with two tables geodkbike and osm_no_bike - or work with OSM edges but if edges are already in OSM_bike, discard the match
-- I want to link the two

For each path line, select the road segments which are within a distance tolerance d
For each matched road segment, "clip" the path line to it (clipping explained below)
Discard any clipped segments which have length 0 (these are segments which are roughly perpendicular to the path, e.g. cross streets)
Compute the Hausdorff distance between the road segment and the clipped path, and keep only segments with a distance below the tolerance d (this discards road segments where only one end is near the path)
Clip each road segment to the path line (to discard pieces of the road segment which extend beyond the path)
Merge the clipped road segments together


create table blue_lines AS                                                                                                            
WITH 
tbla AS (SELECT DISTINCT ST_Buffer(geom, 20, 'endcap=flat join=round') geom FROM red_lines)
(SELECT DISTINCT ST_Intersection(a.geom, b.geom) geom FROM green_lines a JOIN tbla b ON ST_Intersects(a.geom, b.geom))

CREATE TABLE matched_segments AS
    WITH matched AS (SELECT DISTINCT ST_Buffer(geom, 10, 'endcap'))

First try for a small area
Then implement on a grid by grid basis

for each feature/row in geodkbike:
    - find the XXX closests features in OSM within max threshold
    - for each matched OSM feature:
    -   clip GeoDk-feature to the extent of OSM feature -- what happens with non-matched segments?
    - Discard any OSM feature with length 0 (or below some threshold)
    - Compute Hausdorff distance
    - Only keep OSM matches within a specific tolerance - keep all or best match?
    - If length difference between matched OSM segment and matched GEOdk is less than 10 percent:
        - simply keep the match - transfer attribute of GeoDK to OSM feature
    - elif length difference is larger than 10%:
        - clip OSM to length of GeoDK
        - how to deal with clipped segments?
        - Transfer GeoDK attributes to CORRECT OSM feature

    - how to save results??


USE THIS METHOD:

# Loop through all features in GeoDK bike table

# Find the closest features in OSM (candidate list) within 10 meters - as a dataframe?

    # For each matched OSM feature:
        # Compute length of GeoDK features and matched OSM features
        # Compute Hausdorff distance
        # If Hausdorff distance is below threshold and length diff is less than 10%:
            - Consider it a match and transfer GeoDK attribute to OSM 

        # If not, clip GeoDK feature to extent of OSM feature
            # Save discarded GeoDK segment somehow? -- leave for later
        # If length < XXX:
            # Delete segment
        # Again, compute Hausdorff distance
        # Discard OSM features above a XXX tolerance
        # Select best match
        # Transfer GeoDK attribute to OSM