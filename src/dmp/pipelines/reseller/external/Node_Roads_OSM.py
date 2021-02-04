# Load dependencies
import logging

import geopandas as gp
import numpy as np
import pandas as pd

log = logging.getLogger(__name__)


# Define function
def Process_Roads_OSM(
    OSM_roads_lines_df: gp.geodataframe.GeoDataFrame,
) -> gp.geodataframe.GeoDataFrame:

    # Wrangle it
    OSM_roads_lines_df = OSM_roads_lines_df[["fclass", "geometry"]]
    road_types = ["motorway", "trunk", "primary", "secondary"]
    OSM_roads_lines_df = OSM_roads_lines_df[
        OSM_roads_lines_df["fclass"].isin(road_types)
    ]
    OSM_roads_lines_df = OSM_roads_lines_df.replace(["motorway", "trunk"], "primary")
    # OSM_roads_lines_df = OSM_roads_lines_df.sample(n = 100)
    OSM_roads_lines_df = OSM_roads_lines_df.reset_index(drop=True)

    # Extract points
    coordinates_list_of_tuples = []
    for i in range(0, len(OSM_roads_lines_df)):
        if i % 1000 == 0:
            log.info(i)
        coordinates_temp_list_of_tuples = list(OSM_roads_lines_df.iloc[i, 1].coords)
        for j in range(0, len(coordinates_temp_list_of_tuples)):
            coordinates_list_of_tuples.append(coordinates_temp_list_of_tuples[j])
        fclass = OSM_roads_lines_df.iloc[i, 0]
        class_vector = np.repeat(fclass, len(coordinates_temp_list_of_tuples))
        if i == 0:
            class_column = class_vector
        else:
            class_column = np.append(class_column, class_vector)

    # Create dataframe
    road_points_df = pd.DataFrame(
        coordinates_list_of_tuples, columns=["longitude", "latitude"]
    )
    road_points_df["road_type"] = pd.Series(class_column)
    column_titles = ["road_type", "latitude", "longitude"]
    road_points_df = road_points_df.reindex(columns=column_titles)

    # Return
    return road_points_df
