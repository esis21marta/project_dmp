# Packages
import geopandas as gp


# Define function
def Process_Demographics_GAR(
    GAR_df: gp.geodataframe.GeoDataFrame,
) -> gp.geodataframe.GeoDataFrame:

    # Create new columns
    GAR_df["emp_agr"] = GAR_df["emp_agr_pu"] + GAR_df["emp_agr_pr"]
    GAR_df["emp_gov"] = GAR_df["emp_gov_pu"] + GAR_df["emp_gov_pr"]
    GAR_df["emp_ind"] = GAR_df["emp_ind_pu"] + GAR_df["emp_ind_pr"]
    GAR_df["emp_ser"] = GAR_df["emp_ser_pu"] + GAR_df["emp_ser_pr"]

    GAR_df["ic_high"] = GAR_df["ic_high_pu"] + GAR_df["ic_high_pr"]
    GAR_df["ic_mhg"] = GAR_df["ic_mhg_pu"] + GAR_df["ic_mhg_pr"]
    GAR_df["ic_mlw"] = GAR_df["ic_mlw_pu"] + GAR_df["ic_mlw_pr"]
    GAR_df["ic_low"] = GAR_df["ic_low_pu"] + GAR_df["ic_low_pr"]

    GAR_df["Latitude"] = GAR_df.centroid.y
    GAR_df["Longitude"] = GAR_df.centroid.x

    # Select relevant columns in order
    GAR_2_df = GAR_df.loc[
        :,
        [
            "Latitude",
            "Longitude",
            "emp_agr",
            "emp_gov",
            "emp_ind",
            "emp_ser",
            "ic_high",
            "ic_mhg",
            "ic_mlw",
            "ic_low",
            "tot_val",
        ],
    ]

    return GAR_2_df
