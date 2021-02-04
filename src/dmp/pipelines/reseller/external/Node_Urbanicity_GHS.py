import logging
import math

import numpy as np
import pandas as pd

from raster2xyz.raster2xyz import Raster2xyz

log = logging.getLogger(__name__)


# Define function
def Process_Urbanicity_GHS(input_tif: str, temp_csv: str, out_csv: str):

    # Convert raster to csv
    log.info("Converting raster to CSV")
    rtxyz = Raster2xyz()
    rtxyz.translate(input_tif, temp_csv)

    # Load csv
    log.info("Loading CSV")
    Indo_urbanicity_df = pd.read_csv(temp_csv)

    # Filter values out of range of latitude and longitude
    log.info("Filtering coordinates")
    minx, miny = 8538000, -2011589
    maxx, maxy = 16187000, 1524227
    Indo_urbanicity_df = Indo_urbanicity_df[Indo_urbanicity_df.y >= miny]
    Indo_urbanicity_df = Indo_urbanicity_df[Indo_urbanicity_df.y <= maxy]
    Indo_urbanicity_df = Indo_urbanicity_df[Indo_urbanicity_df.x >= minx]
    Indo_urbanicity_df = Indo_urbanicity_df[Indo_urbanicity_df.x <= maxx]

    # Transform coordinates
    log.info("Transforming coordinates")
    R = 6378100
    pi = 3.14159265359
    central_meridian = 0
    x_vector = Indo_urbanicity_df["x"]
    y_vector = Indo_urbanicity_df["y"]
    latitude = np.zeros(len(Indo_urbanicity_df))
    longitude = np.zeros(len(Indo_urbanicity_df))

    for i in range(len(Indo_urbanicity_df)):
        x = x_vector.iloc[i]
        y = y_vector.iloc[i]
        cita = math.asin(y / (R * math.sqrt(2)))
        latitude[i] = math.asin((2 * cita + math.sin(2 * cita)) / pi) * (180 / pi)
        longitude[i] = (
            central_meridian + (pi * x) / (2 * R * math.sqrt(2) * math.cos(cita))
        ) * (180 / pi)

    Indo_urbanicity_df["x"] = longitude
    Indo_urbanicity_df["y"] = latitude

    # Assign column names
    Indo_urbanicity_df.rename(
        columns={"y": "Latitude", "x": "Longitude", "z": "Urbanicity"}, inplace=True
    )
    column_titles = ["Latitude", "Longitude", "Urbanicity"]
    Indo_urbanicity_df = Indo_urbanicity_df.reindex(columns=column_titles)

    # Filter values out of range of latitude and longitude
    minx, miny = 92.412064, -14.326956
    maxx, maxy = 145.755629, 8.900269
    Indo_urbanicity_df = Indo_urbanicity_df[Indo_urbanicity_df.Latitude >= miny]
    Indo_urbanicity_df = Indo_urbanicity_df[Indo_urbanicity_df.Latitude <= maxy]
    Indo_urbanicity_df = Indo_urbanicity_df[Indo_urbanicity_df.Longitude >= minx]
    Indo_urbanicity_df = Indo_urbanicity_df[Indo_urbanicity_df.Longitude <= maxx]

    # Filter urbanicity = 10, which correspond to water bodies
    Indo_urbanicity_df = Indo_urbanicity_df.loc[Indo_urbanicity_df["Urbanicity"] >= 11]

    # Assign urbanicity values from 1 to 7
    Urbanicity = Indo_urbanicity_df["Urbanicity"].tolist()
    for i in range(len(Urbanicity)):
        if Urbanicity[i] == 11:
            Urbanicity[i] = 1
        elif Urbanicity[i] == 12:
            Urbanicity[i] = 2
        elif Urbanicity[i] == 13:
            Urbanicity[i] = 3
        elif Urbanicity[i] == 21:
            Urbanicity[i] = 4
        elif Urbanicity[i] == 22:
            Urbanicity[i] = 5
        elif Urbanicity[i] == 23:
            Urbanicity[i] = 6
        elif Urbanicity[i] == 30:
            Urbanicity[i] = 7
        else:
            Urbanicity[i] = 0
    Indo_urbanicity_df["Urbanicity"] = Urbanicity

    # Save file
    log.info("Saving to CSV")
    Indo_urbanicity_df.to_csv(out_csv, index=False)
