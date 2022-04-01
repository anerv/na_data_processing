# Matching features in road network datasets

- The script matches a reference dataset with edges/lines (R) with a corresponding OSM dataset with edges/lines (O), and has been developed to match different datasets of road networks.

- The initial use case is a situation where attributes from R needs to be transferred to the corresponding edges in O. The method can however also be used for simply identifying which edges in O correspond to the edges in R.

- A match describes the relationship between a feature in R and a feature in O that descrive the same physical object (e.g. the same street).

- The best results are obtained with simplified datasets (e.g. no interstitial nodes between intersections).

## Input

- A (simplified) dataset with reference line features read into a GeoDataFrame
- A (simplified) dataset with OSM line features read into a GeDataFrame
- The segment length
- The buffer distance
- CRS the analysis should be using (should be a projected CRS)
- A max Hausdorff distance for a match to be valid (in meters)
- A max angle between two segments for the match to be valid (in degrees)
- Name of column in reference data containing the attribute that should be transferred to the OSM data. Use the name of the column containing the id if no attribute should be transferred.

## Output

- Step 3 outputs a dataframe with the reference segments and a column indicating the index and one with the id of the OSM segment it has been matched to (if a match was found).
- Step 4 outputs the initial OSM feature dataframe with a column containing the attribute transfered from the reference data.

## Workflow

1. Create segments of each feature in the data of a specified segment length.
2. For each segment in R, find segments in O that are within XX meters of the R segment. These represent potential matches
3. For each segment compute the best match out of potential matches. This step considers the angle and Hausdorff distance to identify corresponding segments. If none are within the specified threshold, no match is found.
4. Updates the original OSM data based on the matches with the reference data. This step assumes that the user wants to transfer an attribute to OSM, but can easily be modified to transfer the id of the matched reference feature.

## Caveats

- There can be a one-to-many relationship between the features in O and the features in R. I.e. one edge in O can be matched to several edges in R.  This is intentional to account for the original use case in which R contains data on cycling infrastructure mapped as lines on each side of a street, while OSM often only has one centerline mapped.
- The functions used in the process expects OSM segments to be uniquely identifed through a column 'osmid'. The segmentation function creates a unique id 'seg_id', which should be renamed to 'osmid' before the analysis starts. If the original osmid are to be used later on, they must be stored in a column with a different name.
- The matching are done at the segment level, but the results are summarised on the feature level. This steps converts the data back to the features level, and sets the matched value that has been matched to the majority of the feature's segments. Summarizing the match at the feature level fixes a lot of issues with segments that are not correctly matched - but if e.g. an OSM edge is matched to two different reference edges with conflicting attributes, only the attribute that is matched to the most feature segments is stored. It is mostly a problem if the reference dataset is more granular than the OSM data.
