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

from typing import List

import pyspark
import pyspark.sql.functions as F


def month_to_MM(month: int) -> str:
    if month > 9:
        return str(month)
    else:
        return "0" + str(month)


def clean_kr(kr: pyspark.sql.DataFrame, year: int, month: int) -> pyspark.sql.DataFrame:
    """
    Filters on the chosen year and month, encodes the flag_bad_61, flags kr customers.

    :param kr: Raw kr table
    :param year: Year for which all data is selected
    :param month: Month for which all data is selected
    :return: Cleaned kr table
    """

    month = month_to_MM(month)
    target_month = f"{year}-{month}"

    kr = (
        kr.withColumn("yearmonth", F.substring(F.col("application_date"), 1, 7))
        .filter(F.col("yearmonth") == target_month)
        .filter(F.col("msisdn") != "first_user_number")
        .drop_duplicates(subset=["msisdn"])
        .withColumn("kr_customer", F.lit(1))
        .withColumn(
            "default_61",
            F.when(F.col("flag_bad_61") == "good", 0).otherwise(
                F.when(F.col("flag_bad_61") == "bad", 1).otherwise(None)
            ),
        )
    )

    columns_of_interest = [
        "msisdn",
        "default_61",
        "kr_customer",
    ]

    return kr.select(columns_of_interest)


def join_tables(
    all_msisdns: pyspark.sql.DataFrame,
    basis: pyspark.sql.DataFrame,
    kr: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    Joins the basis table to all_msisdns, then joins the kr table.

    :param all_msisnds: Table containing all msisdns in the period of interest
    :param basis: Cleaned basis table
    :param kr: Cleaned kr table
    :return: Three tables joined
    """

    return all_msisdns.join(basis, on=["msisdn"], how="left").join(
        F.broadcast(kr), on=["msisdn"], how="left"
    )


def get_cross_tabs(
    master_table: pyspark.sql.DataFrame, groups: List[str]
) -> pyspark.sql.DataFrame:
    """
    Computes cross tabs grouping on the specified groups.

    :param master_table: Table with infomation to be grouped
    :param groups: Columns on which to group
    :return: Cross tabs table
    """

    master_table = (
        master_table.drop("msisdn")
        .groupBy(groups)
        .agg(
            F.count("*").alias("count"),
            F.sum("kr_customer").alias("kr_customer_count"),
            F.sum("default_61").alias("default_61_count"),
        )
    )
    return master_table
