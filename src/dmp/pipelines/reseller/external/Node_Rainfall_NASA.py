import logging

import numpy as np
import pandas as pd

import rasterio
from raster2xyz.raster2xyz import Raster2xyz

log = logging.getLogger(__name__)


# Define function
def Process_Rainfall_NASA(
    input_template: str, data_dir: str, input_raster: str, temp_csv: str, out_csv: str
):

    # Open data template
    template = rasterio.open(input_template)
    template_band1 = np.float64(template.read(1))

    # Average rainfall index in each pixel
    precipitation_count = template_band1 * 0
    for year in range(2013, 2016):
        log.info(year)
        for month in [
            "01",
            "02",
            "03",
            "04",
            "05",
            "06",
            "07",
            "08",
            "09",
            "10",
            "11",
            "12",
        ]:
            log.info(month)
            address = (
                data_dir
                + str(year)
                + "/TRMM_3B43M_"
                + str(year)
                + "-"
                + str(month)
                + "-01_gs_1440x720.TIFF"
            )
            raster = rasterio.open(address)
            raster_band1 = np.float64(raster.read(1))

            precipitation_count = precipitation_count + raster_band1

    precipitation_monthly_avg = precipitation_count / 36

    # Save raster
    out_meta = template.meta
    out_meta.update({"dtype": "float64"})
    new_raster = rasterio.open(input_raster, "w", **out_meta)
    new_raster.write(precipitation_monthly_avg, 1)
    new_raster.close()

    # Convert raster to raw CSV
    rtxyz = Raster2xyz()
    rtxyz.translate(input_raster, temp_csv)

    # Convert raw CSV to nice labeled CSV and save
    precipitation_index_df = pd.read_csv(temp_csv)
    precipitation_index_df.rename(
        columns={"x": "Longitude", "y": "Latitude", "z": "Rainfall_index"}, inplace=True
    )
    list(precipitation_index_df.columns)
    column_titles = ["Latitude", "Longitude", "Rainfall_index"]
    precipitation_index_df.reindex(columns=column_titles)
    precipitation_index_df.to_csv(out_csv, index=False)
