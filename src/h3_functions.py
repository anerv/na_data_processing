import h3
import itertools
from collections import Counter
from shapely.geometry import Polygon

def coords_to_h3(coords, h3_res):

    h3_indices_set = set()

    for c in coords:
        # Index point to h3 at h3_res
        index = h3.geo_to_h3(lat=c[1],lng=c[0], resolution = h3_res)
        # Add index to set
        h3_indices_set.add(index)

    h3_indices = list(h3_indices_set)

    # if len(h3_indices) == 1:

    #     return h3_indices[0]

    return h3_indices

def h3_index_to_geometry(h3_indices, shapely_polys=False):

    polygon_coords = []

    for h in h3_indices:
        
        h3_coords = h3.h3_to_geo_boundary(h=h, geo_json=True)
        polygon_coords.append(h3_coords)

    if shapely_polys:
        
        polys = [Polygon(p) for p in polygon_coords]

        return polys

    return polygon_coords


def h3_fill_line(h3_edge_indices):

    # Function for filling out h3 cells between non-adjacent cells

    if len(h3_edge_indices) < 2:

        return h3_edge_indices

    h3_line = set()

    h3_line.update(h3_edge_indices)

    for i in range(0,len(h3_edge_indices)-1):

        if h3.h3_indexes_are_neighbors(h3_edge_indices[i],h3_edge_indices[i+1]) == False:

            missing_hexs = h3.h3_line(h3_edge_indices[i],h3_edge_indices[i+1])

            h3_line.update(missing_hexs)

    return list(h3_line)


def return_edge_h3_indices(group,hex_id_col, results_dict):

   hex_ids = group[hex_id_col].to_list()
   hex_ids_unpacked = list(itertools.chain(*hex_ids))
   
   results_dict[group.name] = hex_ids_unpacked
