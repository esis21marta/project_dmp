import numpy as np
import pyspark
import pyspark.sql.functions as f
from pyspark.sql.window import Window


def calculate_bin_threshold(
    df: pyspark.sql.DataFrame, column_to_bin: str, num_of_bins: int,
):
    """
    Perform binning using n-tile based on the num_of_bins

    :param df: Dataframe containing data to bin
    :param column_to_bin: The name of the column to be binned
    :param num_of_bins: The number of bins to be produced
    :return: YAML file containing the min, max, and proportion of the column and bins
    """

    df = df.select(column_to_bin)
    df = df.withColumn(
        "bins",
        f.ntile(num_of_bins).over(Window.partitionBy().orderBy(f.col(column_to_bin))),
    )
    binning = (
        df.groupby("bins")
        .agg(
            f.min(f.col(column_to_bin)).alias("min"),
            f.max(f.col(column_to_bin)).alias("max"),
            f.mean(f.col(column_to_bin)).alias("mean"),
            f.count(f.col(column_to_bin)).alias("count"),
        )
        .toPandas()
    )

    binning["max_before"] = binning["max"].shift(1)
    binning["min"] = np.where(
        binning["max_before"].notnull(), binning["max_before"], binning["min"]
    )

    binning["proportion"] = binning["count"] / binning["count"].sum()
    binning_list = binning.set_index(["bins"]).to_dict("record")

    dict_of_bins = {}

    dict_of_bins[column_to_bin] = {}
    n_element = len(binning_list)
    for num, bins in enumerate(binning_list):
        dict_of_bins[column_to_bin][num] = {}
        dict_of_bins[column_to_bin][num]["mean"] = float(np.round(bins["mean"], 4))
        dict_of_bins[column_to_bin][num]["min"] = bins["min"] if num > 0 else "-inf"
        dict_of_bins[column_to_bin][num]["max"] = (
            bins["max"] if num < (n_element - 1) else "inf"
        )
        dict_of_bins[column_to_bin][num]["count"] = bins["count"]
        dict_of_bins[column_to_bin][num]["proportion"] = float(
            np.round(bins["proportion"], 4)
        )

    return dict_of_bins
