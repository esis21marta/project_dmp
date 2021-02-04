# Copyright 2018-present QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited ("QuantumBlack") name and logo
# (either separately or in combination, "QuantumBlack Trademarks") are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
#     or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Dict, List

import pyspark
from pyspark.sql.functions import col, lit, lower, trim


def filter_territory(
    scoring_table: pyspark.sql.DataFrame, territory_table: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:

    """
    Remove rows with msisnds corresponding to partner's existing customers

    :param scoring_table: The name of the scoring table
    :param territory_table: The table containing the filtered location of the user
    :return:
    """

    return scoring_table.join(territory_table, on="msisdn", how="inner")


def remove_partner_existing_customers(
    scoring_table: pyspark.sql.DataFrame,
    partner_existing_customers: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:

    """
    Remove rows with msisnds corresponding to partner's existing customers

    :param scoring_table: The name of the scoring table
    :param partner_existing_customers: The table containing msisdns of partner's existing customers
    :return:
    """

    return scoring_table.join(partner_existing_customers, on="msisdn", how="left_anti")


def filter_out_apple(
    scoring_table: pyspark.sql.DataFrame,
    apple_manufacturer: str,
    apple_devices: List[str],
) -> pyspark.sql.DataFrame:

    """
    Filter out Apple devices

    :param scoring_table: The name of the scoring table
    :param apple_manufacturer: The manufacturer name of Apple devices
    :param apple_devices: The list of apple devices
    :return:
    """

    filter_apple_regex = "|".join([f"({device})" for device in apple_devices])

    scoring_table = scoring_table.withColumn(
        "is_apple", lower(col("fea_handset_make_cur")).contains(apple_manufacturer)
    ).withColumn(
        "is_apple",
        col("is_apple")
        | lower(col("fea_handset_market_name")).rlike(filter_apple_regex),
    )

    return scoring_table


def add_dummy_lookalike_score(
    scoring_table: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:

    """
    Add a dummy Lookalike score to the Dataframe in the column 'training_set_similarity_prediction'

    :param scoring_table: Name of the scoring table
    :return: The Dataframe with an additional column called 'training_set_similarity_prediction' containing dummy score
    """

    return scoring_table.withColumn("training_set_similarity_prediction", lit(1))


def filter_out_null_los(scoring_table: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:

    """
    Returns the filtered MSISDN for which los is not null

    :param scoring_table: Name of the scoring table
    :return: The filtered MSISDN
    """

    return scoring_table.filter(col("fea_los").isNotNull())


def filter_out_low_los(
    scoring_table: pyspark.sql.DataFrame, low_los_threshold: int
) -> pyspark.sql.DataFrame:

    """
    Returns the filtered MSISDN higher than the Low LOS threshold

    :param scoring_table: Name of the scoring table
    :param low_los_threshold: The Low LOS threshold
    :return: The filtered MSISDN
    """

    return scoring_table.filter(col("fea_los") > low_los_threshold)


def filter_out_very_high_los(
    scoring_table: pyspark.sql.DataFrame, high_los_threshold: int
) -> pyspark.sql.DataFrame:

    """
    Returns the filtered MSISDN lower than the High LOS threshold

    :param scoring_table: Name of the scoring table
    :param high_los_threshold: The High LOS threshold
    :return: The filtered MSISDN
    """

    return scoring_table.filter((col("fea_los") < high_los_threshold))


def select_specific_brands(
    scoring_table: pyspark.sql.DataFrame, brand_to_include: List[str]
) -> pyspark.sql.DataFrame:

    """
    Returns the filtered MSISDN based on brands

    :param scoring_table: Name of the scoring table
    :param brand_to_include: The list of brand to include
    :return: The filtered MSISDN
    """

    scoring_table_brand_filter = scoring_table.filter(
        trim(lower(col("fea_custprof_brand"))).isin(brand_to_include)
    )
    return scoring_table_brand_filter


def split_score_table(
    scoring_table: pyspark.sql.DataFrame, previous_campaign_table: pyspark.sql.DataFrame
) -> (pyspark.sql.DataFrame, pyspark.sql.DataFrame):

    """
    Split the scoring table into previous_campaign_score_table and remaining_score_table

    :param scoring_table: Name of the scoring table
    :param previous_campaign_table: Name of the previous campaign table
    :return: a couple of (previous_campaign_score_table, remaining_score_table)
    """
    previous_campaign_msisdn = previous_campaign_table.select("msisdn")
    previous_campaign_msisdn.cache()
    previous_campaign_score_table = scoring_table.join(
        previous_campaign_msisdn, on="msisdn", how="inner"
    )
    remaining_score_table = scoring_table.join(
        previous_campaign_msisdn, on="msisdn", how="left_anti"
    )

    return previous_campaign_score_table, remaining_score_table


def select_random_sample(
    scoring_table: pyspark.sql.DataFrame, num_random_samples: int
) -> pyspark.sql.DataFrame:

    """
    Returns num_of_samples samples randomly

    :param scoring_table: Name of the scoring table
    :param num_random_samples: The number of samples to be included
    :return: The filtered MSISDN
    """

    rows_total = scoring_table.count()
    scoring_table_sample = scoring_table.sample(num_random_samples / rows_total, False)
    return scoring_table_sample


def merge_tables(
    previous_campaign_score_table: pyspark.sql.DataFrame,
    remaining_score_table_sampled: pyspark.sql.DataFrame,
) -> (pyspark.sql.DataFrame, pyspark.sql.DataFrame):

    """
    Merges the two input Dataframes together

    :param previous_campaign_score_table: Rrevious campaign score table
    :param remaining_score_table_sampled: Remaining score table after sampling
    :return: the Merged Dataframe
    """

    return previous_campaign_score_table.union(remaining_score_table_sampled)


def select_right_columns(
    scoring_table: pyspark.sql.DataFrame, columns_to_include: List[str]
) -> pyspark.sql.DataFrame:

    """
    Returns the filtered MSISDN selected on the defined columns

    :param scoring_table: Name of the scoring table
    :param columns_to_include: The columns to be included in the final scoring table
    :return: The filtered MSISDN
    """

    scoring_table_filter_columns = scoring_table.select(columns_to_include)
    return scoring_table_filter_columns


def rename_columns_and_coalesce(
    scoring_table: pyspark.sql.DataFrame,
    columns_to_rename: Dict[str, str],
    final_sample_number_files: int,
) -> pyspark.sql.DataFrame:

    """
    Returns the filtered MSISDN selected on the defined columns

    :param scoring_table: Name of the scoring table
    :param columns_to_rename: The columns to be renamed
    :param final_sample_number_files: number of files we want
    :return: The filtered MSISDN
    """

    for column, column_rename in columns_to_rename.items():
        scoring_table = scoring_table.withColumnRenamed(column, column_rename)
    return scoring_table.coalesce(final_sample_number_files)
