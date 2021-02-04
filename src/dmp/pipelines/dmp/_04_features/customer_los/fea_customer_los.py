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
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

from src.dmp.pipelines.dmp._02_primary.scaffold.create_scaffold import create_date_list
from utils import get_end_date, get_required_output_columns, get_start_date


def generate_msisdn_weekstart(
    df_customer_los_agg: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    Generate a pair of MSISDN - Weekstart
    Args:
        df_customer_los_agg: Customer LOS Aggregation Dataframe
    Returns:
        Dataframe of MSISDN - Weekstart
    """
    first_week_start = get_start_date(partition_column="weekstart").strftime("%Y-%m-%d")
    last_week_start = get_end_date(partition_column="weekstart").strftime("%Y-%m-%d")

    end_date_epoch = 2556143999  # Dec 31, 2050  11:59:59 PM GMT
    spark = SparkSession.builder.getOrCreate()

    weekstart_list = (
        create_date_list(spark, end=end_date_epoch).select("weekstart").dropDuplicates()
    )
    weekstart_list = weekstart_list.filter(
        f.col("weekstart").between(first_week_start, last_week_start)
    )
    msisdn_list = df_customer_los_agg.select("msisdn").dropDuplicates()
    msisdn_weekstart = weekstart_list.crossJoin(msisdn_list)

    return msisdn_weekstart


def fea_customer_los(
    df_customer_los_agg: pyspark.sql.DataFrame,
    df_msisdn_weekstart: pyspark.sql.DataFrame,
    feature_mode: str,
    required_output_features: List[str],
) -> pyspark.sql.DataFrame:

    """
    Generate feature for customer LOS

    Args:
        df_customer_los_agg: Customer LOS Aggregation Dataframe

    Returns:
        Dataframe of Feature LOS
    """
    df_customer_los_agg = df_msisdn_weekstart.join(
        df_customer_los_agg, ["msisdn", "weekstart"], how="left"
    )
    df_week = df_customer_los_agg.withColumn(
        "filled_activation_date_prev",
        f.first("activation_date", True).over(
            Window.partitionBy("msisdn").orderBy("weekstart").rowsBetween(-12, 0)
        ),
    ).withColumn(
        "filled_activation_date_next",
        f.first("activation_date", True).over(
            Window.partitionBy("msisdn").orderBy("weekstart").rowsBetween(0, 12)
        ),
    )

    df_week = df_week.withColumn(
        "activation_date",
        f.coalesce(
            f.col("activation_date"),
            f.col("filled_activation_date_prev"),
            f.col("filled_activation_date_next"),
        ),
    ).withColumn(
        "date_diff",
        f.datediff(
            f.to_date(f.col("weekstart"), "yyyy-MM-dd"),
            f.to_date(f.col("activation_date"), "yyyy-MM-dd"),
        ),
    )

    df_week = df_week.filter(f.col("date_diff") > 0)

    fea_los_df = df_week.groupBy("msisdn", "weekstart").agg(
        f.min("date_diff").alias("fea_los")
    )

    required_output_columns = get_required_output_columns(
        output_features=fea_los_df.columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["msisdn", "weekstart"],
    )

    fea_los_df = fea_los_df.select(required_output_columns)

    first_week_start = get_start_date(partition_column="weekstart").strftime("%Y-%m-%d")
    last_week_start = get_end_date(partition_column="weekstart").strftime("%Y-%m-%d")

    return fea_los_df.filter(
        f.col("weekstart").between(first_week_start, last_week_start)
    )
