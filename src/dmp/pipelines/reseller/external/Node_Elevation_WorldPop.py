# Load dependencies
import pandas as pd

from raster2xyz.raster2xyz import Raster2xyz


# Define function
def Process_Elevation_WorldPop(input_raster: str, temp_csv: str, out_csv: str):

    # Convert raster
    rtxyz = Raster2xyz()
    rtxyz.translate(input_raster, temp_csv)

    # Load csv and assign column names
    Elevation_df = pd.read_csv(temp_csv)
    Elevation_df.rename(
        columns={"y": "Latitude", "x": "Longitude", "z": "Elev_m"}, inplace=True
    )

    # Filter values out of range
    Elevation_df = Elevation_df[Elevation_df.Elev_m >= 0]
    Elevation_df = Elevation_df[Elevation_df.Elev_m <= 9000]

    # Save as CSV
    Elevation_df.to_csv(out_csv, index=False)
