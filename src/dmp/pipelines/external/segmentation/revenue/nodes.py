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

import pyspark
import pyspark.sql.functions as F
from pyspark.ml.feature import QuantileDiscretizer

from src.dmp.pipelines.external.segmentation.common.nodes import (
    get_cross_tabs,
    month_to_MM,
)


def _clean_revenue_table(
    revenue_table: pyspark.sql.DataFrame,
    year: int,
    month: int,
    old_revenue_column_name: str,
    new_revenue_column_name: str,
) -> pyspark.sql.DataFrame:
    """
    Filters on the chosen year and month and computes the monthy revenue.

    :param demography: Raw table
    :param year: Year for which data is selected
    :param month: Month for which data is selected
    :param old_revenue_column_name: Revenue column name in the original table
    :param new_revenue_column_name: Revenue column name in the output table
    :return: Cleaned revenue table
    """
    month = month_to_MM(month)
    target_month = f"{year}-{month}"

    revenue_table = (
        revenue_table.withColumn("yearmonth", F.substring(F.col("event_date"), 1, 7))
        .filter(F.col("yearmonth") == target_month)
        .groupBy(F.col("msisdn"), F.col("yearmonth"))
        .agg(F.sum(F.col(old_revenue_column_name)).alias(new_revenue_column_name),)
        .repartition(500)
    )

    columns_of_interest = [
        "msisdn",
        new_revenue_column_name,
    ]

    return revenue_table.select(columns_of_interest)


def clean_payu(
    payu: pyspark.sql.DataFrame, year: int, month: int
) -> pyspark.sql.DataFrame:
    """
    Filters on the chosen year and month and computes the monthy payu revenue.

    :param demography: Raw payu table
    :param year: Year for which data is selected
    :param month: Month for which data is selected
    :return: Cleaned payu table
    """
    return _clean_revenue_table(
        payu,
        year,
        month,
        old_revenue_column_name="rev_total",
        new_revenue_column_name="payu_revenue",
    )


def clean_pkg(
    pkg: pyspark.sql.DataFrame, year: int, month: int
) -> pyspark.sql.DataFrame:
    """
    Filters on the chosen year and month and computes the monthy pkg revenue.

    :param demography: Raw pkg table
    :param year: Year for which data is selected
    :param month: Month for which data is selected
    :return: Cleaned pkg table
    """
    return _clean_revenue_table(
        pkg,
        year,
        month,
        old_revenue_column_name="rev_pkg_prchse",
        new_revenue_column_name="pkg_revenue",
    )


def sum_revenues(
    payu_clean: pyspark.sql.DataFrame, pkg_clean: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:
    """
    Computes revenue_total by adding up revenues from different sources.

    :param payu_clean: Cleaned payu table
    :param pkg_clean: Cleaned pkg table
    :return: Table with revenue_total column
    """
    return (
        payu_clean.join(pkg_clean, on=["msisdn"], how="outer")
        .fillna(0)
        .withColumn("revenue_total", F.col("payu_revenue") + F.col("pkg_revenue"))
        .select("msisdn", "revenue_total")
    )


def summarize_revenues(revenue_sums: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Returns a summary for revenue_total.

    :param revenue_sums: Table with revenue_total column
    :return: Summary of revenue_total
    """
    return (
        revenue_sums.select(F.col("revenue_total"))
        .summary(
            "count",
            "min",
            "10%",
            "20%",
            "30%",
            "40%",
            "50%",
            "60%",
            "70%",
            "80%",
            "90%",
            "max",
        )
        .coalesce(1)
    )


def bin_revenues(revenue_sums: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Bins revenue_total. Missing values are put in a separate bin.

    :param revenue_sums: Table with revenue_total column
    :return: Table with revenue_binned column
    """
    discretizer = QuantileDiscretizer(
        numBuckets=10,
        inputCol="revenue_total",
        outputCol="revenue_binned",
        handleInvalid="keep",
    )

    return discretizer.fit(revenue_sums).transform(revenue_sums)


def get_revenue_cross_tabs(
    master_table: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    Computes cross tabs grouping by revenue data.

    :param master_table: Table containing kr and revenue information
    :return: Cross tabs table
    """

    groups = [
        "revenue_binned",
    ]

    return get_cross_tabs(master_table, groups).coalesce(1)
