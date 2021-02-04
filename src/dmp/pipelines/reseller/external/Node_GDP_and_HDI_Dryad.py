# Load dependencies
import logging

import numpy as np
import pandas as pd

import netCDF4

log = logging.getLogger(__name__)


# Define function
def Process_HDI_Dryad(input_1_nc: str, out_1_csv: str):

    # ------- HDI ---------
    log.info("processing HDI")

    # Read input_1_nc
    nc = netCDF4.Dataset(input_1_nc, "r")
    nc.variables.keys()

    # Variable unique values
    lat = nc.variables["latitude"][:]
    lat_vals = lat.data

    lon = nc.variables["longitude"][:]
    lon_vals = lon.data

    # time = nc.variables['time'][:]
    # Var = nc.variables['HDI'][:]

    # Retrieve HDI 2015 grid
    Var_vals = np.array(nc.variables["HDI"][25, :, :])
    nc.close()

    # Convert grid to dataframe
    Var_vector = np.zeros(len(lon) * len(lat))
    lat_vector = np.zeros(len(lon) * len(lat))
    lon_vector = np.zeros(len(lon) * len(lat))

    for x in range(0, len(lon)):
        for y in range(0, len(lat)):
            Var_vector[len(lon) * y + x] = Var_vals[y, x]
            lat_vector[len(lon) * y + x] = lat_vals[y]
            lon_vector[len(lon) * y + x] = lon_vals[x]

    dict = {"Latitude": lat_vector, "Longitude": lon_vector, "HDI": Var_vector}
    HDI_df = pd.DataFrame(dict)

    # Filter values out of range
    minx, miny = 92.412064, -14.326956
    maxx, maxy = 145.755629, 8.900269
    HDI_df = HDI_df[HDI_df.Latitude >= miny]
    HDI_df = HDI_df[HDI_df.Latitude <= maxy]
    HDI_df = HDI_df[HDI_df.Longitude >= minx]
    HDI_df = HDI_df[HDI_df.Longitude <= maxx]

    HDI_df = HDI_df[HDI_df.HDI >= 0]

    # Save to CSV
    HDI_df.to_csv(out_1_csv, index=False)


# Define function
def Process_GDP_Dryad(input_2_nc: str, out_2_csv: str):
    # ------- GDP ---------
    log.info("processing GDP")

    # Read input_2_nc
    nc = netCDF4.Dataset(input_2_nc, "r")
    nc.variables.keys()

    # Variable unique values
    lat = nc.variables["latitude"][:]
    lat_vals = lat.data

    lon = nc.variables["longitude"][:]
    lon_vals = lon.data

    # time = nc.variables['time'][:]
    # Var = nc.variables['GDP_PPP'][:]

    # Retrieve GDP_PPP 2015 grid
    Var_vals = np.array(nc.variables["GDP_PPP"][2, :, :])
    nc.close()

    # Convert grid to dataframe
    Var_vector = np.zeros(len(lon) * len(lat))
    lat_vector = np.zeros(len(lon) * len(lat))
    lon_vector = np.zeros(len(lon) * len(lat))

    for x in range(0, len(lon)):
        for y in range(0, len(lat)):
            Var_vector[len(lon) * y + x] = Var_vals[y, x]
            lat_vector[len(lon) * y + x] = lat_vals[y]
            lon_vector[len(lon) * y + x] = lon_vals[x]

    dict = {"Latitude": lat_vector, "Longitude": lon_vector, "GDP_PPP": Var_vector}
    GDP_PPP_df = pd.DataFrame(dict)

    # Filter values out of range
    minx, miny = 92.412064, -14.326956
    maxx, maxy = 145.755629, 8.900269
    GDP_PPP_df = GDP_PPP_df[GDP_PPP_df.Latitude >= miny]
    GDP_PPP_df = GDP_PPP_df[GDP_PPP_df.Latitude <= maxy]
    GDP_PPP_df = GDP_PPP_df[GDP_PPP_df.Longitude >= minx]
    GDP_PPP_df = GDP_PPP_df[GDP_PPP_df.Longitude <= maxx]

    GDP_PPP_df = GDP_PPP_df[GDP_PPP_df.GDP_PPP >= 0]

    # Save to CSV
    GDP_PPP_df.to_csv(out_2_csv, index=False)


# Define function
def Process_GDP_per_cap_Dryad(input_3_nc: str, out_3_csv: str):
    # ------- GDP per capita ---------
    log.info("processing GDP per capita")

    # Read input_3_nc
    nc = netCDF4.Dataset(input_3_nc, "r")
    nc.variables.keys()

    # Variable unique values
    lat = nc.variables["latitude"][:]
    lat_vals = lat.data

    lon = nc.variables["longitude"][:]
    lon_vals = lon.data

    # time = nc.variables['time'][:]
    # Var = nc.variables['GDP_per_capita_PPP'][:]

    # Retrieve GDP_per_capita_PPP 2015 grid
    Var_vals = np.array(nc.variables["GDP_per_capita_PPP"][25, :, :])
    nc.close()

    # Convert grid to dataframe
    Var_vector = np.zeros(len(lon) * len(lat))
    lat_vector = np.zeros(len(lon) * len(lat))
    lon_vector = np.zeros(len(lon) * len(lat))

    for x in range(0, len(lon)):
        for y in range(0, len(lat)):
            Var_vector[len(lon) * y + x] = Var_vals[y, x]
            lat_vector[len(lon) * y + x] = lat_vals[y]
            lon_vector[len(lon) * y + x] = lon_vals[x]

    dict = {
        "Latitude": lat_vector,
        "Longitude": lon_vector,
        "GDP_per_capita_PPP": Var_vector,
    }
    GDP_per_capita_PPP_df = pd.DataFrame(dict)

    # Filter values out of range
    minx, miny = 92.412064, -14.326956
    maxx, maxy = 145.755629, 8.900269
    GDP_per_capita_PPP_df = GDP_per_capita_PPP_df[
        GDP_per_capita_PPP_df.Latitude >= miny
    ]
    GDP_per_capita_PPP_df = GDP_per_capita_PPP_df[
        GDP_per_capita_PPP_df.Latitude <= maxy
    ]
    GDP_per_capita_PPP_df = GDP_per_capita_PPP_df[
        GDP_per_capita_PPP_df.Longitude >= minx
    ]
    GDP_per_capita_PPP_df = GDP_per_capita_PPP_df[
        GDP_per_capita_PPP_df.Longitude <= maxx
    ]

    GDP_per_capita_PPP_df = GDP_per_capita_PPP_df[
        GDP_per_capita_PPP_df.GDP_per_capita_PPP >= 0
    ]

    # Save to CSV
    GDP_per_capita_PPP_df.to_csv(out_3_csv, index=False)
