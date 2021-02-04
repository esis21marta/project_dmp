# Load dependencies
import logging

import numpy as np
import pandas as pd

from raster2xyz.raster2xyz import Raster2xyz

log = logging.getLogger(__name__)


# Define function
def Process_dependency_ratio(
    dependency_ratio_raster: str, de_temp: str, dependency_ratio_csv: str
):

    # Convert raster to CSVs
    log.info("Converting dep ratio raster")
    de_rtxyz = Raster2xyz()
    de_rtxyz.translate(dependency_ratio_raster, de_temp)

    # Clean dataset and assign column names
    log.info("Clean datasets and assign column names")
    dependency_ratio_df = pd.read_csv(de_temp)
    column_titles = ["x", "y", "z"]
    dependency_ratio_df = dependency_ratio_df.reindex(columns=column_titles)
    dependency_ratio_df.rename(
        columns={"x": "Longitude", "y": "Latitude", "z": "Dependency_ratio"},
        inplace=True,
    )
    dependency_ratio_df = dependency_ratio_df[dependency_ratio_df.Dependency_ratio >= 0]
    dependency_ratio_df.insert(
        loc=0, column="ID", value=np.arange(len(dependency_ratio_df))
    )

    # Save file
    dependency_ratio_df.to_csv(dependency_ratio_csv, index=False)


# Define function
def Process_births(births_raster: str, bi_temp: str, births_csv: str):

    # Convert raster to CSVs
    log.info("Converting births raster")
    bi_rtxyz = Raster2xyz()
    bi_rtxyz.translate(births_raster, bi_temp)

    # Clean dataset and assign column names
    log.info("Clean datasets and assign column names")
    births_df = pd.read_csv(bi_temp)
    column_titles = ["x", "y", "z"]
    births_df = births_df.reindex(columns=column_titles)
    births_df.rename(
        columns={"x": "Longitude", "y": "Latitude", "z": "Births"}, inplace=True
    )
    births_df = births_df[births_df.Births >= 0]
    births_df.insert(loc=0, column="ID", value=np.arange(len(births_df)))

    # Save file
    births_df.to_csv(births_csv, index=False)


# Define function
def Process_pregnancies(pregnancies_raster: str, pr_temp: str, pregnancies_csv: str):

    # Convert raster to CSVs
    log.info("Converting pregnancies raster")
    pr_rtxyz = Raster2xyz()
    pr_rtxyz.translate(pregnancies_raster, pr_temp)

    # Clean dataset and assign column names
    log.info("Clean datasets and assign column names")
    pregnancies_df = pd.read_csv(pr_temp)
    column_titles = ["x", "y", "z"]
    pregnancies_df = pregnancies_df.reindex(columns=column_titles)
    pregnancies_df.rename(
        columns={"x": "Longitude", "y": "Latitude", "z": "Pregnancies"}, inplace=True
    )
    pregnancies_df = pregnancies_df[pregnancies_df.Pregnancies >= 0]
    pregnancies_df.insert(loc=0, column="ID", value=np.arange(len(pregnancies_df)))

    # Save file
    pregnancies_df.to_csv(pregnancies_csv, index=False)


# Define function
def Merge_all_datasets(
    dependency_ratio_csv: str,
    births_csv: str,
    pregnancies_csv: str,
    merged_data_csv: str,
):

    # Clean dataset and assign column names
    log.info("Clean datasets and assign column names")
    dependency_ratio_df = pd.read_csv(dependency_ratio_csv)
    births_df = pd.read_csv(births_csv)
    pregnancies_df = pd.read_csv(pregnancies_csv)

    # Merge births and pregnancies datasets
    bi_pr_df = pd.concat([births_df, pregnancies_df], axis=1)
    bi_pr_df.columns = [
        "ID2",
        "Longitude2",
        "Latitude2",
        "Births",
        "ID",
        "Longitude",
        "Latitude",
        "Pregnancies",
    ]
    bi_pr_2_df = bi_pr_df[["Longitude", "Latitude", "Births", "Pregnancies"]]

    # Round lat-longs
    dependency_ratio_df["Longitude"] = dependency_ratio_df["Longitude"].round(6)
    dependency_ratio_df["Latitude"] = dependency_ratio_df["Latitude"].round(6)
    bi_pr_2_df["Longitude"] = bi_pr_2_df["Longitude"].round(6)
    bi_pr_2_df["Latitude"] = bi_pr_2_df["Latitude"].round(6)

    # Merge the 3 datasets
    merged_df = pd.merge(
        dependency_ratio_df,
        bi_pr_2_df,
        how="left",
        left_on=["Longitude", "Latitude"],
        right_on=["Longitude", "Latitude"],
    )
    merged_df = merged_df.fillna(0)

    # Filter no-data points
    merged_2_df = merged_df.loc[
        (merged_df.Dependency_ratio >= 0)
        | (merged_df.Births >= 0)
        | (merged_df.Pregnancies >= 0)
    ]
    merged_2_df["Dependency_ratio"] = np.where(
        (merged_2_df.Dependency_ratio < 0.0), 0.0, merged_2_df.Dependency_ratio
    )
    merged_2_df["Births"] = np.where(
        (merged_2_df.Births < 0.0), 0.0, merged_2_df.Births
    )
    merged_2_df["Pregnancies"] = np.where(
        (merged_2_df.Pregnancies < 0.0), 0.0, merged_2_df.Pregnancies
    )

    # Drop ID column
    merged_2_df = merged_2_df.drop(columns=["ID"])

    # Save file
    merged_2_df.to_csv(merged_data_csv, index=False)
