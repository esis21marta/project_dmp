# Packages
import logging

import geopandas as gp
import numpy as np
import pandas as pd
from scipy.spatial import distance
from shapely.geometry import Point

import pygeohash as pgh
from pygeodesy.geohash import neighbors

log = logging.getLogger(__name__)


# Define function
def Process_Cellphone_Signal_OpenCellID(
    WP_1km_grid_gdf: gp.geodataframe.GeoDataFrame,
    cell_phone_signal_df: pd.core.frame.DataFrame,
) -> gp.geodataframe.GeoDataFrame:

    # Clean WorldPop grid
    WP_1km_grid_gdf.insert(loc=0, column="ID", value=np.arange(len(WP_1km_grid_gdf)))
    column_titles = ["ID", "Latitude", "Longitude", "geometry"]
    WP_1km_grid_gdf = WP_1km_grid_gdf.reindex(columns=column_titles)

    # Load and clean cell phone signal data
    cell_phone_signal_df = cell_phone_signal_df[["radio", "lon", "lat"]]
    cell_phone_signal_df.rename(
        columns={"radio": "Tech", "lon": "Longitude", "lat": "Latitude"}, inplace=True
    )  # inplace True is for returning a new dataframe
    column_titles = ["Tech", "Latitude", "Longitude"]
    cell_phone_signal_df = cell_phone_signal_df.reindex(columns=column_titles)
    geometry = [
        Point(xy)
        for xy in zip(cell_phone_signal_df.Longitude, cell_phone_signal_df.Latitude)
    ]
    crs = {"init": "epsg:4326"}
    cell_phone_signal_gdf = gp.GeoDataFrame(
        cell_phone_signal_df, crs=crs, geometry=geometry
    )
    cell_phone_signal_gdf.insert(
        loc=0, column="ID", value=np.arange(len(cell_phone_signal_gdf))
    )

    # Add geohashes (running time ~4 mins)
    WP_1km_grid_gdf["Geohash"] = WP_1km_grid_gdf.apply(
        lambda x: pgh.encode(x.Latitude, x.Longitude, precision=4), axis=1
    )
    cell_phone_signal_gdf["Geohash"] = cell_phone_signal_gdf.apply(
        lambda x: pgh.encode(x.Latitude, x.Longitude, precision=4), axis=1
    )

    # Find unique WP geohashes
    unique_geohashes = WP_1km_grid_gdf.Geohash.unique()

    # Prepare datasets
    WP_1km_grid_gdf["Closest_LTE_km"] = -1
    WP_1km_grid_gdf["Closest_GSM_km"] = -1
    WP_1km_grid_gdf["Closest_UMTS_km"] = -1
    WP_1km_grid_gdf["Closest_CDMA_km"] = -1

    # ------ For each WP point, find closest LTE signal point
    log.info("--> working on LTE")

    # -- For each geohash, find all WP points
    LTE_gdf = cell_phone_signal_gdf.loc[cell_phone_signal_gdf["Tech"] == "LTE"]

    indices = np.empty(0, dtype=int)
    distances = np.empty(0, dtype=float)

    for i in range(0, len(unique_geohashes)):

        # -- Counter
        if i % 100 == 0:
            log.info(i)

        # -- Unique geohashes
        g = unique_geohashes[i]

        # -- Filter WP points
        WP_g_gdf = WP_1km_grid_gdf.loc[WP_1km_grid_gdf["Geohash"] == g]

        # -- Find geohash neighbors
        Neighbors_1 = neighbors(g)
        Grid_3x3_list = [
            g,
            Neighbors_1.N,
            Neighbors_1.NE,
            Neighbors_1.E,
            Neighbors_1.SE,
            Neighbors_1.S,
            Neighbors_1.SW,
            Neighbors_1.W,
            Neighbors_1.NW,
        ]
        Grid_3x3 = np.array(Grid_3x3_list)

        # -- Try to find cell phone points in 3x3 grid
        LTE_g_gdf = LTE_gdf[LTE_gdf["Geohash"].isin(Grid_3x3)]
        points = len(LTE_g_gdf)

        if points >= 1:
            # -- For each WP point, find closest cell phone point for each technology
            Lat1 = WP_g_gdf["Latitude"].tolist()
            Lon1 = WP_g_gdf["Longitude"].tolist()
            P1 = np.transpose(np.asarray([Lat1, Lon1], dtype=np.float32))

            Lat2 = LTE_g_gdf["Latitude"].tolist()
            Lon2 = LTE_g_gdf["Longitude"].tolist()
            P2 = np.transpose(np.asarray([Lat2, Lon2], dtype=np.float32))

            Distance_vector = distance.cdist(P1, P2).min(axis=1) * 111

            WP_g_gdf["Closest_LTE_km"] = Distance_vector
        else:
            WP_g_gdf["Closest_LTE_km"] = 85

        indices = np.append(indices, WP_g_gdf["Closest_LTE_km"].index)
        distances = np.append(distances, WP_g_gdf["Closest_LTE_km"])

    # WP_1km_grid_gdf['Closest_LTE_km'][indices] = distances
    WP_1km_grid_gdf.loc[indices, "Closest_LTE_km"] = distances

    # ------ For each WP point, find closest GSM signal point
    log.info("--> working on GSM")

    # -- For each geohash, find all WP points
    GSM_gdf = cell_phone_signal_gdf.loc[cell_phone_signal_gdf["Tech"] == "GSM"]

    indices = np.empty(0, dtype=int)
    distances = np.empty(0, dtype=float)

    for i in range(0, len(unique_geohashes)):

        # -- Counter
        if i % 100 == 0:
            log.info(i)

        # -- Unique geohashes
        g = unique_geohashes[i]

        # -- Filter WP points
        WP_g_gdf = WP_1km_grid_gdf.loc[WP_1km_grid_gdf["Geohash"] == g]

        # -- Find geohash neighbors
        Neighbors_1 = neighbors(g)
        Grid_3x3_list = [
            g,
            Neighbors_1.N,
            Neighbors_1.NE,
            Neighbors_1.E,
            Neighbors_1.SE,
            Neighbors_1.S,
            Neighbors_1.SW,
            Neighbors_1.W,
            Neighbors_1.NW,
        ]
        Grid_3x3 = np.array(Grid_3x3_list)

        # -- Try to find cell phone points in 3x3 grid
        GSM_g_gdf = GSM_gdf[GSM_gdf["Geohash"].isin(Grid_3x3)]
        points = len(GSM_g_gdf)

        if points >= 1:
            # -- For each WP point, find closest cell phone point for each technology
            Lat1 = WP_g_gdf["Latitude"].tolist()
            Lon1 = WP_g_gdf["Longitude"].tolist()
            P1 = np.transpose(np.asarray([Lat1, Lon1], dtype=np.float32))

            Lat2 = GSM_g_gdf["Latitude"].tolist()
            Lon2 = GSM_g_gdf["Longitude"].tolist()
            P2 = np.transpose(np.asarray([Lat2, Lon2], dtype=np.float32))

            Distance_vector = distance.cdist(P1, P2).min(axis=1) * 111

            WP_g_gdf["Closest_GSM_km"] = Distance_vector
        else:
            WP_g_gdf["Closest_GSM_km"] = 85

        indices = np.append(indices, WP_g_gdf["Closest_GSM_km"].index)
        distances = np.append(distances, WP_g_gdf["Closest_GSM_km"])

        # Increase counter
        i += 1

    # WP_1km_grid_gdf['Closest_GSM_km'][indices] = distances
    WP_1km_grid_gdf.loc[indices, "Closest_GSM_km"] = distances

    # ------ For each WP point, find closest UMTS signal point
    log.info("--> working on UMTS")

    # -- For each geohash, find all WP points
    UMTS_gdf = cell_phone_signal_gdf.loc[cell_phone_signal_gdf["Tech"] == "UMTS"]

    indices = np.empty(0, dtype=int)
    distances = np.empty(0, dtype=float)
    i = 0  # Counter
    for i in range(0, len(unique_geohashes)):

        # -- Counter
        if i % 100 == 0:
            log.info(i)

        # -- Unique geohashes
        g = unique_geohashes[i]

        # -- Filter WP points
        WP_g_gdf = WP_1km_grid_gdf.loc[WP_1km_grid_gdf["Geohash"] == g]

        # -- Find geohash neighbors
        Neighbors_1 = neighbors(g)
        Grid_3x3_list = [
            g,
            Neighbors_1.N,
            Neighbors_1.NE,
            Neighbors_1.E,
            Neighbors_1.SE,
            Neighbors_1.S,
            Neighbors_1.SW,
            Neighbors_1.W,
            Neighbors_1.NW,
        ]
        Grid_3x3 = np.array(Grid_3x3_list)

        # -- Try to find cell phone points in 3x3 grid
        UMTS_g_gdf = UMTS_gdf[UMTS_gdf["Geohash"].isin(Grid_3x3)]
        points = len(UMTS_g_gdf)

        if points >= 1:
            # -- For each WP point, find closest cell phone point for each technology
            Lat1 = WP_g_gdf["Latitude"].tolist()
            Lon1 = WP_g_gdf["Longitude"].tolist()
            P1 = np.transpose(np.asarray([Lat1, Lon1], dtype=np.float32))

            Lat2 = UMTS_g_gdf["Latitude"].tolist()
            Lon2 = UMTS_g_gdf["Longitude"].tolist()
            P2 = np.transpose(np.asarray([Lat2, Lon2], dtype=np.float32))

            Distance_vector = distance.cdist(P1, P2).min(axis=1) * 111

            WP_g_gdf["Closest_UMTS_km"] = Distance_vector
        else:
            WP_g_gdf["Closest_UMTS_km"] = 85

        indices = np.append(indices, WP_g_gdf["Closest_UMTS_km"].index)
        distances = np.append(distances, WP_g_gdf["Closest_UMTS_km"])

        # Increase counter
        i += 1

    # WP_1km_grid_gdf['Closest_UMTS_km'][indices] = distances
    WP_1km_grid_gdf.loc[indices, "Closest_UMTS_km"] = distances

    # ------ For each WP point, find closest CDMA signal point
    log.info("--> working on CDMA")

    # -- For each geohash, find all WP points
    CDMA_gdf = cell_phone_signal_gdf.loc[cell_phone_signal_gdf["Tech"] == "CDMA"]

    indices = np.empty(0, dtype=int)
    distances = np.empty(0, dtype=float)
    i = 0  # Counter
    for i in range(0, len(unique_geohashes)):

        # -- Counter
        if i % 100 == 0:
            log.info(i)

        # -- Unique geohashes
        g = unique_geohashes[i]

        # -- Filter WP points
        WP_g_gdf = WP_1km_grid_gdf.loc[WP_1km_grid_gdf["Geohash"] == g]

        # -- Find geohash neighbors
        Neighbors_1 = neighbors(g)
        Grid_3x3_list = [
            g,
            Neighbors_1.N,
            Neighbors_1.NE,
            Neighbors_1.E,
            Neighbors_1.SE,
            Neighbors_1.S,
            Neighbors_1.SW,
            Neighbors_1.W,
            Neighbors_1.NW,
        ]
        Grid_3x3 = np.array(Grid_3x3_list)

        # -- Try to find cell phone points in 3x3 grid
        CDMA_g_gdf = CDMA_gdf[CDMA_gdf["Geohash"].isin(Grid_3x3)]
        points = len(CDMA_g_gdf)

        if points >= 1:
            # -- For each WP point, find closest cell phone point for each technology
            Lat1 = WP_g_gdf["Latitude"].tolist()
            Lon1 = WP_g_gdf["Longitude"].tolist()
            P1 = np.transpose(np.asarray([Lat1, Lon1], dtype=np.float32))

            Lat2 = CDMA_g_gdf["Latitude"].tolist()
            Lon2 = CDMA_g_gdf["Longitude"].tolist()
            P2 = np.transpose(np.asarray([Lat2, Lon2], dtype=np.float32))

            Distance_vector = distance.cdist(P1, P2).min(axis=1) * 111

            WP_g_gdf["Closest_CDMA_km"] = Distance_vector
        else:
            WP_g_gdf["Closest_CDMA_km"] = 85

        indices = np.append(indices, WP_g_gdf["Closest_CDMA_km"].index)
        distances = np.append(distances, WP_g_gdf["Closest_CDMA_km"])

        # Increase counter
        i += 1

    # WP_1km_grid_gdf['Closest_CDMA_km'][indices] = distances
    WP_1km_grid_gdf.loc[indices, "Closest_CDMA_km"] = distances

    return WP_1km_grid_gdf
