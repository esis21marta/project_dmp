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

import logging
from datetime import timedelta

import pyspark
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

from utils import get_config_parameters, get_end_date, get_start_date
from utils.spark_data_set_helper import get_file_path

log = logging.getLogger(__name__)


def union_datasets(
    cb_pre_dd: pyspark.sql.DataFrame,
    cb_post_dd: pyspark.sql.DataFrame,
    t_cb_full_hist: pyspark.sql.DataFrame,
    cb_prior_start_date: str,
    cb_prior_end_date: str,
) -> pyspark.sql.DataFrame:
    """
    Union the prepaid, postpaid, and old prepaid_postpaid DB. Also creating
    a flag whether the MSISDN is prepaid or postpaid.

    Args:
        cb_pre_dd: Dataframe for cb_pre_dd DB.
        cb_post_dd: Dataframe for cb_post_dd DB.
        t_cb_full_hist: Dataframe for t_cb_full_hist DB.
        cb_prior_start_date: CB Start Date
        cb_prior_end_date: CB End Date

    Returns:
        Dataframe with prepaid and postpaid information.
    """
    df_pre = cb_pre_dd.withColumn("prepost_flag", f.lit("prepaid"))
    df_post = cb_post_dd.withColumn("prepost_flag", f.lit("postpaid"))
    cols_select = ["msisdn", "activation_date", "prepost_flag", "event_date"]

    df_pre_post = df_pre.select(cols_select).union(df_post.select(cols_select))

    df_prior_mar_2019 = (
        t_cb_full_hist.filter(
            f.col("event_date").between(cb_prior_start_date, cb_prior_end_date)
        )
        .withColumnRenamed("prepost_label", "prepost_flag")
        .filter(f.col("prepost_flag") != "unknown")
    )

    df_pre_post = df_pre_post.select(cols_select).union(
        df_prior_mar_2019.select(cols_select)
    )

    return df_pre_post


def assign_window_features(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Assign each rows in Dataframe with previous row information i.e prev_activation_date,
    prev_event_date, prev_prepost_flag, activation_date_diff, event_date_diff

    Args:
        df: Dataframe to be assigned with previous row information.

    Returns:
        Dataframe with previous row information.
    """
    # assign previous activation_date
    df = df.withColumn(
        "prev_activation_date",
        f.lag(f.col("activation_date")).over(
            Window.partitionBy("msisdn").orderBy("event_date", "prepost_flag")
        ),
    )
    df = df.withColumn(
        "prev_activation_date",
        f.when(
            f.col("prev_activation_date").isNull(), f.col("activation_date")
        ).otherwise(f.col("prev_activation_date")),
    )

    # also assign prev_event_date
    df = df.withColumn(
        "prev_event_date",
        f.lag(f.col("event_date")).over(
            Window.partitionBy("msisdn").orderBy("event_date", "prepost_flag")
        ),
    )
    df = df.withColumn(
        "prev_event_date",
        f.when(f.col("prev_event_date").isNull(), f.col("event_date")).otherwise(
            f.col("prev_event_date")
        ),
    )

    # assign prev pre_post_flag
    df = df.withColumn(
        "prev_prepost_flag",
        f.lag(f.col("prepost_flag")).over(
            Window.partitionBy("msisdn").orderBy("event_date", "prepost_flag")
        ),
    )
    df = df.withColumn(
        "prev_prepost_flag",
        f.when(f.col("prev_prepost_flag").isNull(), f.col("prepost_flag")).otherwise(
            f.col("prev_prepost_flag")
        ),
    )

    # calculate the difference of activation date
    df = df.withColumn(
        "act_diff",
        f.datediff(
            f.to_date(f.col("activation_date"), "yyyy-MM-dd"),
            f.to_date(f.col("prev_activation_date"), "yyyy-MM-dd"),
        ),
    )

    # calculate the difference of event date
    df = df.withColumn(
        "ev_diff",
        f.datediff(
            f.to_date(f.col("event_date"), "yyyy-MM-dd"),
            f.to_date(f.col("prev_event_date"), "yyyy-MM-dd"),
        ),
    )

    return df


def los_validation(
    df: pyspark.sql.DataFrame, churn_period: int
) -> pyspark.sql.DataFrame:
    """
    Validate the activation_date, ignore activation date if:
    - prepaid to postpaid migration
    - suddenly the activation date changes (less than churn_period)
    Args:
        df: Dataframe to validate activation_date.
        churn_period: Churn period of MSISDN

    Returns:
        Dataframe with valid activation_date.
    """
    df = df.withColumn(
        "ignore_activation_date",
        f.when(
            (f.col("prev_prepost_flag") == "prepaid")
            & (f.col("prepost_flag") == "postpaid")
            & (f.col("act_diff") > 0)
            & (f.col("ev_diff") < churn_period),
            f.lit(1),
        )
        .when((f.col("act_diff") > 0) & (f.col("ev_diff") < churn_period), f.lit(1))
        .when(
            (f.col("act_diff") != 0)
            & (f.col("ev_diff") < churn_period)
            & (f.col("prev_prepost_flag") == f.col("prepost_flag")),
            f.lit(1),
        )
        .when(
            (f.col("ev_diff") < churn_period)
            & (f.col("prev_prepost_flag") == "postpaid")
            & (f.col("prepost_flag") == "prepaid"),
            f.lit(1),
        )
        .when(f.col("activation_date") < "1995-01-01", f.lit(1))
        .otherwise(f.lit(0)),
    )

    # only select activation date which doesn't have ignore_activation_date = 1
    los_df = (
        df.select("msisdn", "activation_date", "ignore_activation_date", "weekstart")
        .groupby("msisdn", "activation_date", "weekstart")
        .agg(f.sum("ignore_activation_date").alias("sum_ignore_act_date"))
        .filter(f.col("sum_ignore_act_date") == 0)
    )

    return los_df.select("msisdn", "activation_date", "weekstart")


def get_previous_valid_activation_date(df):
    """
    For each MSISDN, get previous valid activation_date from weekly aggregation
    Args:
        df: Dataframe of Weekly Aggregation

    Returns:
        Dataframe with one valid activation_date per MSISDN
    """
    df_prev_valid = (
        df.withColumn(
            "activation_date_list",
            f.collect_list("activation_date").over(
                Window.partitionBy("msisdn").orderBy("weekstart")
            ),
        )
        .groupBy("msisdn")
        .agg(f.max("activation_date_list").alias("activation_date_list"))
    )
    df_prev_valid = df_prev_valid.select(
        "msisdn",
        f.element_at(f.col("activation_date_list"), -1).alias("activation_date"),
    )
    return df_prev_valid


def _weekly_aggregation_first_weekstart(df_data, weekstart, churn_period):
    """
    Aggregates customer_los to msisdn, weekstart, only used when weekstart is
    2019-01-07

    Args:
        df_data: Dataframe of Union data source
        weekstart: Weekstart
        churn_period: Customer churn period

    Returns:
        Dataframe of Weekly Aggregated data
    """
    df_assign = assign_window_features(df_data).withColumn(
        "weekstart", f.lit(weekstart)
    )
    df_valid_date = los_validation(df_assign, churn_period)

    df_agg = df_valid_date.groupBy("msisdn", "weekstart").agg(
        f.max("activation_date").alias("activation_date")
    )

    return df_agg


def _weekly_aggregation(df, prev_valid, weekstart):
    """
    Aggregates customer_los to msisdn, weekstart

    Args:
        df: Dataframe of Union data source
        prev_valid: Dataframe of last 3 months valid activation date
        weekstart: weekstart

    Returns:
        Dataframe of Weekly Aggregated data
    """
    prev_valid = get_previous_valid_activation_date(prev_valid)

    df_agg = (
        df.withColumn(
            "activation_date_list",
            f.collect_list("activation_date").over(
                Window.partitionBy("msisdn").orderBy(
                    f.col("prepost_flag").desc(), "event_date"
                )
            ),
        )
        .groupBy("msisdn")
        .agg(f.max("activation_date_list").alias("activation_date_list"))
    )

    df_agg = df_agg.select(
        "msisdn",
        f.element_at(f.col("activation_date_list"), 1).alias("activation_date"),
    )

    df_agg = df_agg.withColumn("weekstart", f.lit(weekstart))
    df_joined = df_agg.join(
        prev_valid.withColumnRenamed("activation_date", "prev_activation_date").drop(
            "weekstart"
        ),
        ["msisdn"],
        how="left",
    )
    df_joined = df_joined.withColumn(
        "activation_date",
        f.coalesce(f.col("prev_activation_date"), f.col("activation_date")),
    ).drop("prev_activation_date")

    return df_joined


def create_weekly_customer_los_table(
    t_cb_full_hist: pyspark.sql.DataFrame,
    cb_pre_dd: pyspark.sql.DataFrame,
    cb_post_dd: pyspark.sql.DataFrame,
    cb_prior_start_date: str,
    cb_prior_end_date: str,
    churn_period: int,
) -> None:
    """
    Aggregates customer_los to msisdn, weekstart

    Args:
        t_cb_full_hist: Old data for prepaid & postpaid
        cb_pre_dd: Prepaid data
        cb_post_dd: Postpaid data
        cb_prior_start_date: Old data start date
        cb_prior_end_date: Old data end date
        churn_period: Customer churn period
    """
    conf_catalog = get_config_parameters(config="catalog")
    spark = SparkSession.builder.getOrCreate()

    start_date = get_start_date()
    end_date = get_end_date()

    load_args = conf_catalog["l1_prepaid_customers_data"]["load_args"]

    weekly_agg_catalog = conf_catalog["l1_customer_los_weekly"]

    save_args = weekly_agg_catalog["save_args"]
    save_args.pop("partitionBy", None)
    file_path = get_file_path(filepath=weekly_agg_catalog["filepath"])
    file_format = weekly_agg_catalog["file_format"]
    partitions = int(weekly_agg_catalog["partitions"])

    df_data = union_datasets(
        cb_pre_dd, cb_post_dd, t_cb_full_hist, cb_prior_start_date, cb_prior_end_date,
    )
    log.info(
        "Starting Weekly Aggregation for WeekStart {start_date} to {end_date}".format(
            start_date=(start_date + timedelta(days=7)).strftime("%Y-%m-%d"),
            end_date=(end_date + timedelta(days=1)).strftime("%Y-%m-%d"),
        )
    )
    log.info(f"File Path: {file_path}")
    log.info(f"File Format: {file_format}")
    log.info(f"Load Args: {load_args}")
    log.info(f"Save Args: {save_args}")
    log.info(f"Partitions: {partitions}")

    while start_date < end_date:
        week_start = (start_date + timedelta(days=7)).strftime("%Y-%m-%d")
        sdate = start_date.strftime(load_args["partition_date_format"])
        edate = (start_date + timedelta(days=6)).strftime(
            load_args["partition_date_format"]
        )
        log.info("Starting Weekly Aggregation for WeekStart: {}".format(week_start))

        df_filtered = df_data.filter(
            f.col(load_args["partition_column"]).between(sdate, edate)
        )

        if week_start == "2019-01-07":
            sdate = (start_date - timedelta(days=90)).strftime(
                load_args["partition_date_format"]
            )
            df_filtered = df_data.filter(
                f.col(load_args["partition_column"]).between(sdate, edate)
            )
            df = _weekly_aggregation_first_weekstart(
                df_filtered, week_start, churn_period
            )
        else:
            df_agg_weekly = spark.read.parquet(file_path)
            weekstart_from = (start_date - timedelta(days=84)).strftime(
                "%Y-%m-%d"
            )  # churn period is 90 (less than 91), so we take previous week before churn
            weekstart_to = sdate
            prev_valid = df_agg_weekly.filter(
                f.col("weekstart").between(weekstart_from, weekstart_to)
            )
            df = _weekly_aggregation(df_filtered, prev_valid, week_start)

        df = df.drop(f.col("weekstart"))

        partition_file_path = "{file_path}/weekstart={weekstart}".format(
            file_path=file_path, weekstart=week_start
        )

        df.repartition(numPartitions=partitions).write.save(
            partition_file_path, file_format, **save_args
        )
        log.info(f"Completed Weekly Aggregation for WeekStart: {week_start}")
        start_date += timedelta(days=7)
