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
import pyspark.sql.types as t

from utils import (
    get_config_parameters,
    get_end_date,
    get_start_date,
    get_window_pby_msisdn_oby_trx_date,
    next_week_start_day,
    union_all,
)
from utils.spark_data_set_helper import get_file_path

log = logging.getLogger(__name__)


def generate_revenue_pkg_rcg(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Generic function for rev_pkg_dd and rev_rcg_dd data transformations

    Args:
        :param df: rev_pkg_dd or rev_rcg_dd daily level table

    Returns:
        df: Transformed rev_pkg_dd or rev_rcg_dd data
    """
    df = (
        df.withColumn("rev_payu_tot", f.lit(0))
        .withColumn("rev_payu_voice", f.lit(0))
        .withColumn("rev_payu_sms", f.lit(0))
        .withColumn("rev_payu_data", f.lit(0))
        .withColumn("rev_payu_roam", f.lit(0))
        .withColumn("rev_payu_dls", f.lit(0))
        .withColumn("rev_payu_dls_mms_cnt", f.lit(0))
        .withColumn("rev_payu_dls_mms_p2p", f.lit(0))
        .withColumn("rev_payu_dls_music", f.lit(0))
        .withColumn("rev_payu_dls_other_vas", f.lit(0))
        .withColumn("rev_payu_dls_rbt", f.lit(0))
        .withColumn("rev_payu_dls_sms_nonp2p", f.lit(0))
        .withColumn("rev_payu_dls_tp", f.lit(0))
        .withColumn("rev_payu_dls_ussd", f.lit(0))
        .withColumn("rev_payu_dls_videocall", f.lit(0))
        .withColumn("rev_payu_dls_voice_nonp2p", f.lit(0))
        .withColumn("rev_pkg_tot", f.col("rev_pkg_prchse"))
        .withColumn("rev_pkg_voice", f.col("rev_voice_pkg_prchs"))
        .withColumn("rev_pkg_sms", f.col("rev_sms_pkg_prchs"))
        .withColumn("rev_pkg_data", f.col("rev_data_pkg_prchs"))
        .withColumn("rev_pkg_roam", f.col("rev_roam_pkg_prchs"))
        .select(
            "msisdn",
            "event_date",
            "rev_voice_pkg_prchs",
            "rev_data_pkg_prchs",
            "rev_sms_pkg_prchs",
            "rev_payu_tot",
            "rev_payu_voice",
            "rev_payu_sms",
            "rev_payu_data",
            "rev_payu_roam",
            "rev_payu_dls",
            "rev_payu_dls_mms_cnt",
            "rev_payu_dls_mms_p2p",
            "rev_payu_dls_music",
            "rev_payu_dls_other_vas",
            "rev_payu_dls_rbt",
            "rev_payu_dls_sms_nonp2p",
            "rev_payu_dls_tp",
            "rev_payu_dls_ussd",
            "rev_payu_dls_videocall",
            "rev_payu_dls_voice_nonp2p",
            "rev_pkg_tot",
            "rev_pkg_voice",
            "rev_pkg_sms",
            "rev_pkg_data",
            "rev_pkg_roam",
        )
    )
    return df


def _weekly_aggregation(
    df_rev_payu_dd: pyspark.sql.DataFrame,
    df_rev_pkg_dd: pyspark.sql.DataFrame,
    df_rev_rcg_dd: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    Creates weekly recharge table by:
        1. Unions rev_payu_dd, rev_pkg_dd, and rev_rcg_dd
        2. Aggregates tot_trx and tot_amt to daily level for each msisdn
        2. Aggregates tot_trx and tot_amt to weekly level for each msisdn

    Args:
        :param df_rev_payu_dd: rev_payu_dd daily level table
        :param df_rev_pkg_dd: rev_pkg_dd daily level table
        :param df_rev_rcg_dd: rev_rcg_dd daily level table

    Returns:
        df_agg: Weekly aggregated recharge table
    """
    df_rev_payu_dd = (
        df_rev_payu_dd.withColumn("rev_payu_tot", f.col("rev_total"))
        .withColumn("rev_payu_voice", f.col("rev_voice"))
        .withColumn("rev_payu_sms", f.col("rev_sms"))
        .withColumn("rev_payu_data", f.col("rev_data"))
        .withColumn("rev_payu_roam", f.col("rev_roaming"))
        .withColumn("rev_payu_dls", f.col("rev_dls"))
        .withColumn("rev_payu_dls_mms_cnt", f.col("rev_dls_mms_cnt"))
        .withColumn("rev_payu_dls_mms_p2p", f.col("rev_dls_mms_p2p"))
        .withColumn("rev_payu_dls_music", f.col("rev_dls_music"))
        .withColumn("rev_payu_dls_other_vas", f.col("rev_dls_other_vas"))
        .withColumn("rev_payu_dls_rbt", f.col("rev_dls_rbt"))
        .withColumn("rev_payu_dls_sms_nonp2p", f.col("rev_dls_sms_nonp2p"))
        .withColumn("rev_payu_dls_tp", f.col("rev_dls_tp"))
        .withColumn("rev_payu_dls_ussd", f.col("rev_dls_ussd"))
        .withColumn("rev_payu_dls_videocall", f.col("rev_dls_videocall"))
        .withColumn("rev_payu_dls_voice_nonp2p", f.col("rev_dls_voice_nonp2p"))
        .withColumn("rev_pkg_tot", f.lit(0))
        .withColumn("rev_pkg_voice", f.lit(0))
        .withColumn("rev_pkg_sms", f.lit(0))
        .withColumn("rev_pkg_data", f.lit(0))
        .withColumn("rev_pkg_roam", f.lit(0))
        .select(
            "msisdn",
            "event_date",
            "rev_voice",
            "rev_data",
            "rev_sms",
            "rev_payu_tot",
            "rev_payu_voice",
            "rev_payu_sms",
            "rev_payu_data",
            "rev_payu_roam",
            "rev_payu_dls",
            "rev_payu_dls_mms_cnt",
            "rev_payu_dls_mms_p2p",
            "rev_payu_dls_music",
            "rev_payu_dls_other_vas",
            "rev_payu_dls_rbt",
            "rev_payu_dls_sms_nonp2p",
            "rev_payu_dls_tp",
            "rev_payu_dls_ussd",
            "rev_payu_dls_videocall",
            "rev_payu_dls_voice_nonp2p",
            "rev_pkg_tot",
            "rev_pkg_voice",
            "rev_pkg_sms",
            "rev_pkg_data",
            "rev_pkg_roam",
        )
    )

    df_rev_pkg_dd = generate_revenue_pkg_rcg(df_rev_pkg_dd)
    df_rev_rcg_dd = generate_revenue_pkg_rcg(df_rev_rcg_dd)

    df_union = union_all(df_rev_payu_dd, df_rev_pkg_dd, df_rev_rcg_dd)

    # daily aggregation to distinct count threshold
    df_daily_agg = (
        df_union.groupBy("msisdn", "event_date")
        .agg(
            f.sum("rev_voice").cast(t.LongType()).alias("rev_voice"),
            f.sum("rev_data").cast(t.LongType()).alias("rev_data"),
            f.sum("rev_sms").cast(t.LongType()).alias("rev_sms"),
            f.sum("rev_payu_tot").cast(t.LongType()).alias("rev_payu_tot"),
            f.sum("rev_payu_voice").cast(t.LongType()).alias("rev_payu_voice"),
            f.sum("rev_payu_sms").cast(t.LongType()).alias("rev_payu_sms"),
            f.sum("rev_payu_data").cast(t.LongType()).alias("rev_payu_data"),
            f.sum("rev_payu_roam").cast(t.LongType()).alias("rev_payu_roam"),
            f.sum("rev_payu_dls").cast(t.LongType()).alias("rev_payu_dls"),
            f.sum("rev_payu_dls_mms_cnt")
            .cast(t.LongType())
            .alias("rev_payu_dls_mms_cnt"),
            f.sum("rev_payu_dls_mms_p2p")
            .cast(t.LongType())
            .alias("rev_payu_dls_mms_p2p"),
            f.sum("rev_payu_dls_music").cast(t.LongType()).alias("rev_payu_dls_music"),
            f.sum("rev_payu_dls_other_vas")
            .cast(t.LongType())
            .alias("rev_payu_dls_other_vas"),
            f.sum("rev_payu_dls_rbt").cast(t.LongType()).alias("rev_payu_dls_rbt"),
            f.sum("rev_payu_dls_sms_nonp2p")
            .cast(t.LongType())
            .alias("rev_payu_dls_sms_nonp2p"),
            f.sum("rev_payu_dls_tp").cast(t.LongType()).alias("rev_payu_dls_tp"),
            f.sum("rev_payu_dls_ussd").cast(t.LongType()).alias("rev_payu_dls_ussd"),
            f.sum("rev_payu_dls_videocall")
            .cast(t.LongType())
            .alias("rev_payu_dls_videocall"),
            f.sum("rev_payu_dls_voice_nonp2p")
            .cast(t.LongType())
            .alias("rev_payu_dls_voice_nonp2p"),
            f.sum("rev_pkg_tot").cast(t.LongType()).alias("rev_pkg_tot"),
            f.sum("rev_pkg_voice").cast(t.LongType()).alias("rev_pkg_voice"),
            f.sum("rev_pkg_sms").cast(t.LongType()).alias("rev_pkg_sms"),
            f.sum("rev_pkg_data").cast(t.LongType()).alias("rev_pkg_data"),
            f.sum("rev_pkg_roam").cast(t.LongType()).alias("rev_pkg_roam"),
            f.countDistinct(
                f.when(f.col("rev_payu_voice").between(100, 999), "event_date")
            ).alias("days_with_rev_payu_voice_above_99_below_1000"),
            f.countDistinct(
                f.when(f.col("rev_payu_voice").between(1000, 4999), "event_date")
            ).alias("days_with_rev_payu_voice_above_999_below_5000"),
            f.countDistinct(
                f.when(f.col("rev_payu_voice").between(5000, 9999), "event_date")
            ).alias("days_with_rev_payu_voice_above_4999_below_10000"),
            f.countDistinct(
                f.when(f.col("rev_payu_voice").between(10000, 49999), "event_date")
            ).alias("days_with_rev_payu_voice_above_9999_below_50000"),
            f.countDistinct(
                f.when(f.col("rev_payu_voice") >= 50000, "event_date")
            ).alias("days_with_rev_payu_voice_above_49999"),
            f.countDistinct(
                f.when(f.col("rev_payu_data").between(100, 999), "event_date")
            ).alias("days_with_rev_payu_data_above_99_below_1000"),
            f.countDistinct(
                f.when(f.col("rev_payu_data").between(1000, 4999), "event_date")
            ).alias("days_with_rev_payu_data_above_999_below_5000"),
            f.countDistinct(
                f.when(f.col("rev_payu_data").between(5000, 9999), "event_date")
            ).alias("days_with_rev_payu_data_above_4999_below_10000"),
            f.countDistinct(
                f.when(f.col("rev_payu_data").between(10000, 49999), "event_date")
            ).alias("days_with_rev_payu_data_above_9999_below_50000"),
            f.countDistinct(
                f.when(f.col("rev_payu_data") >= 50000, "event_date")
            ).alias("days_with_rev_payu_data_above_49999"),
            f.countDistinct(
                f.when(f.col("rev_payu_sms").between(100, 999), "event_date")
            ).alias("days_with_rev_payu_sms_above_99_below_1000"),
            f.countDistinct(
                f.when(f.col("rev_payu_sms").between(1000, 4999), "event_date")
            ).alias("days_with_rev_payu_sms_above_999_below_5000"),
            f.countDistinct(
                f.when(f.col("rev_payu_sms").between(5000, 9999), "event_date")
            ).alias("days_with_rev_payu_sms_above_4999_below_10000"),
            f.countDistinct(
                f.when(f.col("rev_payu_sms").between(10000, 49999), "event_date")
            ).alias("days_with_rev_payu_sms_above_9999_below_50000"),
            f.countDistinct(f.when(f.col("rev_payu_sms") >= 50000, "event_date")).alias(
                "days_with_rev_payu_sms_above_49999"
            ),
            f.countDistinct(
                f.when(f.col("rev_payu_dls").between(100, 999), "event_date")
            ).alias("days_with_rev_payu_dls_above_99_below_1000"),
            f.countDistinct(
                f.when(f.col("rev_payu_dls").between(1000, 4999), "event_date")
            ).alias("days_with_rev_payu_dls_above_999_below_5000"),
            f.countDistinct(
                f.when(f.col("rev_payu_dls").between(5000, 9999), "event_date")
            ).alias("days_with_rev_payu_dls_above_4999_below_10000"),
            f.countDistinct(
                f.when(f.col("rev_payu_dls").between(10000, 49999), "event_date")
            ).alias("days_with_rev_payu_dls_above_9999_below_50000"),
            f.countDistinct(f.when(f.col("rev_payu_dls") >= 50000, "event_date")).alias(
                "days_with_rev_payu_dls_above_49999"
            ),
            f.countDistinct(
                f.when(f.col("rev_payu_roam").between(100, 999), "event_date")
            ).alias("days_with_rev_payu_roam_above_99_below_1000"),
            f.countDistinct(
                f.when(f.col("rev_payu_roam").between(1000, 4999), "event_date")
            ).alias("days_with_rev_payu_roam_above_999_below_5000"),
            f.countDistinct(
                f.when(f.col("rev_payu_roam").between(5000, 9999), "event_date")
            ).alias("days_with_rev_payu_roam_above_4999_below_10000"),
            f.countDistinct(
                f.when(f.col("rev_payu_roam").between(10000, 49999), "event_date")
            ).alias("days_with_rev_payu_roam_above_9999_below_50000"),
            f.countDistinct(
                f.when(f.col("rev_payu_roam") >= 50000, "event_date")
            ).alias("days_with_rev_payu_roam_above_49999"),
        )
        .withColumn("weekstart", next_week_start_day(f.col("event_date")))
        .withColumn("is_weekend", f.dayofweek("event_date").isin([1, 7]).cast("int"))
    )

    df_daily_agg = calculate_internal_days_since_last_rev(df_daily_agg)
    df_daily_agg = calculate_internal_spend_salary(df_daily_agg)

    # weekly (as final) aggregation
    df_weekly_agg = df_daily_agg.groupBy("msisdn", "weekstart").agg(
        f.sum("rev_voice").cast(t.LongType()).alias("rev_voice"),
        f.sum("rev_data").cast(t.LongType()).alias("rev_data"),
        f.sum("rev_sms").cast(t.LongType()).alias("rev_sms"),
        f.sum(f.when(f.col("is_weekend") == 1, f.col("rev_voice")))
        .cast(t.LongType())
        .alias("rev_voice_weekend"),
        f.sum(f.when(f.col("is_weekend") == 1, f.col("rev_data")))
        .cast(t.LongType())
        .alias("rev_data_weekend"),
        f.sum(f.when(f.col("is_weekend") == 1, f.col("rev_sms")))
        .cast(t.LongType())
        .alias("rev_sms_weekend"),
        f.sum("rev_payu_tot").cast(t.LongType()).alias("rev_payu_tot"),
        f.sum("rev_payu_voice").cast(t.LongType()).alias("rev_payu_voice"),
        f.sum("rev_payu_sms").cast(t.LongType()).alias("rev_payu_sms"),
        f.sum("rev_payu_data").cast(t.LongType()).alias("rev_payu_data"),
        f.sum("rev_payu_roam").cast(t.LongType()).alias("rev_payu_roam"),
        f.sum("rev_payu_dls").cast(t.LongType()).alias("rev_payu_dls"),
        f.sum("rev_payu_dls_mms_cnt").cast(t.LongType()).alias("rev_payu_dls_mms_cnt"),
        f.sum("rev_payu_dls_mms_p2p").cast(t.LongType()).alias("rev_payu_dls_mms_p2p"),
        f.sum("rev_payu_dls_music").cast(t.LongType()).alias("rev_payu_dls_music"),
        f.sum("rev_payu_dls_other_vas")
        .cast(t.LongType())
        .alias("rev_payu_dls_other_vas"),
        f.sum("rev_payu_dls_rbt").cast(t.LongType()).alias("rev_payu_dls_rbt"),
        f.sum("rev_payu_dls_sms_nonp2p")
        .cast(t.LongType())
        .alias("rev_payu_dls_sms_nonp2p"),
        f.sum("rev_payu_dls_tp").cast(t.LongType()).alias("rev_payu_dls_tp"),
        f.sum("rev_payu_dls_ussd").cast(t.LongType()).alias("rev_payu_dls_ussd"),
        f.sum("rev_payu_dls_videocall")
        .cast(t.LongType())
        .alias("rev_payu_dls_videocall"),
        f.sum("rev_payu_dls_voice_nonp2p")
        .cast(t.LongType())
        .alias("rev_payu_dls_voice_nonp2p"),
        f.sum("rev_pkg_tot").cast(t.LongType()).alias("rev_pkg_tot"),
        f.sum("rev_pkg_voice").cast(t.LongType()).alias("rev_pkg_voice"),
        f.sum("rev_pkg_sms").cast(t.LongType()).alias("rev_pkg_sms"),
        f.sum("rev_pkg_data").cast(t.LongType()).alias("rev_pkg_data"),
        f.sum("rev_pkg_roam").cast(t.LongType()).alias("rev_pkg_roam"),
        f.max(f.col("max_last_rev_date_payu")).alias("max_date_payu"),
        f.max(f.col("max_last_rev_date_pkg")).alias("max_date_pkg"),
        f.max(f.col("event_date")).alias("max_date_tot"),
        f.avg("days_since_last_rev_payu")
        .cast(t.LongType())
        .alias("days_since_last_rev_payu"),
        f.avg("days_since_last_rev_pkg")
        .cast(t.LongType())
        .alias("days_since_last_rev_pkg"),
        f.avg("days_since_last_rev_tot")
        .cast(t.LongType())
        .alias("days_since_last_rev_tot"),
        f.sum("rev_voice_tot_1_5d").cast(t.LongType()).alias("rev_voice_tot_1_5d"),
        f.sum("rev_voice_tot_6_10d").cast(t.LongType()).alias("rev_voice_tot_6_10d"),
        f.sum("rev_voice_tot_11_15d").cast(t.LongType()).alias("rev_voice_tot_11_15d"),
        f.sum("rev_voice_tot_16_20d").cast(t.LongType()).alias("rev_voice_tot_16_20d"),
        f.sum("rev_voice_tot_21_25d").cast(t.LongType()).alias("rev_voice_tot_21_25d"),
        f.sum("rev_voice_tot_26_31d").cast(t.LongType()).alias("rev_voice_tot_26_31d"),
        f.sum("rev_data_tot_1_5d").cast(t.LongType()).alias("rev_data_tot_1_5d"),
        f.sum("rev_data_tot_6_10d").cast(t.LongType()).alias("rev_data_tot_6_10d"),
        f.sum("rev_data_tot_11_15d").cast(t.LongType()).alias("rev_data_tot_11_15d"),
        f.sum("rev_data_tot_16_20d").cast(t.LongType()).alias("rev_data_tot_16_20d"),
        f.sum("rev_data_tot_21_25d").cast(t.LongType()).alias("rev_data_tot_21_25d"),
        f.sum("rev_data_tot_26_31d").cast(t.LongType()).alias("rev_data_tot_26_31d"),
        f.sum("rev_sms_tot_1_5d").cast(t.LongType()).alias("rev_sms_tot_1_5d"),
        f.sum("rev_sms_tot_6_10d").cast(t.LongType()).alias("rev_sms_tot_6_10d"),
        f.sum("rev_sms_tot_11_15d").cast(t.LongType()).alias("rev_sms_tot_11_15d"),
        f.sum("rev_sms_tot_16_20d").cast(t.LongType()).alias("rev_sms_tot_16_20d"),
        f.sum("rev_sms_tot_21_25d").cast(t.LongType()).alias("rev_sms_tot_21_25d"),
        f.sum("rev_sms_tot_26_31d").cast(t.LongType()).alias("rev_sms_tot_26_31d"),
        f.sum("rev_roam_tot_1_5d").cast(t.LongType()).alias("rev_roam_tot_1_5d"),
        f.sum("rev_roam_tot_6_10d").cast(t.LongType()).alias("rev_roam_tot_6_10d"),
        f.sum("rev_roam_tot_11_15d").cast(t.LongType()).alias("rev_roam_tot_11_15d"),
        f.sum("rev_roam_tot_16_20d").cast(t.LongType()).alias("rev_roam_tot_16_20d"),
        f.sum("rev_roam_tot_21_25d").cast(t.LongType()).alias("rev_roam_tot_21_25d"),
        f.sum("rev_roam_tot_26_31d").cast(t.LongType()).alias("rev_roam_tot_26_31d"),
        f.sum("days_with_rev_payu_voice_above_99_below_1000")
        .cast(t.LongType())
        .alias("days_with_rev_payu_voice_above_99_below_1000"),
        f.sum("days_with_rev_payu_voice_above_999_below_5000")
        .cast(t.LongType())
        .alias("days_with_rev_payu_voice_above_999_below_5000"),
        f.sum("days_with_rev_payu_voice_above_4999_below_10000")
        .cast(t.LongType())
        .alias("days_with_rev_payu_voice_above_4999_below_10000"),
        f.sum("days_with_rev_payu_voice_above_9999_below_50000")
        .cast(t.LongType())
        .alias("days_with_rev_payu_voice_above_9999_below_50000"),
        f.sum("days_with_rev_payu_voice_above_49999")
        .cast(t.LongType())
        .alias("days_with_rev_payu_voice_above_49999"),
        f.sum("days_with_rev_payu_data_above_99_below_1000")
        .cast(t.LongType())
        .alias("days_with_rev_payu_data_above_99_below_1000"),
        f.sum("days_with_rev_payu_data_above_999_below_5000")
        .cast(t.LongType())
        .alias("days_with_rev_payu_data_above_999_below_5000"),
        f.sum("days_with_rev_payu_data_above_4999_below_10000")
        .cast(t.LongType())
        .alias("days_with_rev_payu_data_above_4999_below_10000"),
        f.sum("days_with_rev_payu_data_above_9999_below_50000")
        .cast(t.LongType())
        .alias("days_with_rev_payu_data_above_9999_below_50000"),
        f.sum("days_with_rev_payu_data_above_49999")
        .cast(t.LongType())
        .alias("days_with_rev_payu_data_above_49999"),
        f.sum("days_with_rev_payu_sms_above_99_below_1000")
        .cast(t.LongType())
        .alias("days_with_rev_payu_sms_above_99_below_1000"),
        f.sum("days_with_rev_payu_sms_above_999_below_5000")
        .cast(t.LongType())
        .alias("days_with_rev_payu_sms_above_999_below_5000"),
        f.sum("days_with_rev_payu_sms_above_4999_below_10000")
        .cast(t.LongType())
        .alias("days_with_rev_payu_sms_above_4999_below_10000"),
        f.sum("days_with_rev_payu_sms_above_9999_below_50000")
        .cast(t.LongType())
        .alias("days_with_rev_payu_sms_above_9999_below_50000"),
        f.sum("days_with_rev_payu_sms_above_49999")
        .cast(t.LongType())
        .alias("days_with_rev_payu_sms_above_49999"),
        f.sum("days_with_rev_payu_dls_above_99_below_1000")
        .cast(t.LongType())
        .alias("days_with_rev_payu_dls_above_99_below_1000"),
        f.sum("days_with_rev_payu_dls_above_999_below_5000")
        .cast(t.LongType())
        .alias("days_with_rev_payu_dls_above_999_below_5000"),
        f.sum("days_with_rev_payu_dls_above_4999_below_10000")
        .cast(t.LongType())
        .alias("days_with_rev_payu_dls_above_4999_below_10000"),
        f.sum("days_with_rev_payu_dls_above_9999_below_50000")
        .cast(t.LongType())
        .alias("days_with_rev_payu_dls_above_9999_below_50000"),
        f.sum("days_with_rev_payu_dls_above_49999")
        .cast(t.LongType())
        .alias("days_with_rev_payu_dls_above_49999"),
        f.sum("days_with_rev_payu_roam_above_99_below_1000")
        .cast(t.LongType())
        .alias("days_with_rev_payu_roam_above_99_below_1000"),
        f.sum("days_with_rev_payu_roam_above_999_below_5000")
        .cast(t.LongType())
        .alias("days_with_rev_payu_roam_above_999_below_5000"),
        f.sum("days_with_rev_payu_roam_above_4999_below_10000")
        .cast(t.LongType())
        .alias("days_with_rev_payu_roam_above_4999_below_10000"),
        f.sum("days_with_rev_payu_roam_above_9999_below_50000")
        .cast(t.LongType())
        .alias("days_with_rev_payu_roam_above_9999_below_50000"),
        f.sum("days_with_rev_payu_roam_above_49999")
        .cast(t.LongType())
        .alias("days_with_rev_payu_roam_above_49999"),
    )

    return df_weekly_agg


def calculate_internal_days_since_last_rev(df_daily_agg):
    return (
        df_daily_agg.withColumn(
            "last_rev_date_payu", f.when(f.col("rev_payu_tot") > 0, f.col("event_date"))
        )
        .withColumn(
            "max_last_rev_date_payu",
            f.last(f.col("last_rev_date_payu"), ignorenulls=True).over(
                get_window_pby_msisdn_oby_trx_date("msisdn", "event_date")
            ),
        )
        .withColumn(
            "prev_date_payu",
            f.lag(f.col("max_last_rev_date_payu")).over(
                get_window_pby_msisdn_oby_trx_date("msisdn", "event_date")
            ),
        )
        .withColumn(
            "last_rev_date_pkg", f.when(f.col("rev_pkg_tot") > 0, f.col("event_date"))
        )
        .withColumn(
            "max_last_rev_date_pkg",
            f.last(f.col("last_rev_date_pkg"), ignorenulls=True).over(
                get_window_pby_msisdn_oby_trx_date("msisdn", "event_date")
            ),
        )
        .withColumn(
            "prev_date_pkg",
            f.lag(f.col("max_last_rev_date_pkg")).over(
                get_window_pby_msisdn_oby_trx_date("msisdn", "event_date")
            ),
        )
        .withColumn(
            "prev_date",
            f.lag(f.col("event_date")).over(
                get_window_pby_msisdn_oby_trx_date("msisdn", "event_date")
            ),
        )
        .withColumn(
            "days_since_last_rev_payu",
            f.datediff(f.col("event_date"), f.col("prev_date_payu")),
        )
        .withColumn(
            "days_since_last_rev_pkg",
            f.datediff(f.col("event_date"), f.col("prev_date_pkg")),
        )
        .withColumn(
            "days_since_last_rev_tot",
            f.datediff(f.col("event_date"), f.col("prev_date")),
        )
    )


def calculate_internal_spend_salary(df_daily_agg):
    return (
        df_daily_agg.withColumn(
            "rev_voice_tot_1_5d",
            f.when(
                f.dayofmonth(f.col("event_date")).between(1, 5),
                f.col("rev_payu_voice") + f.col("rev_pkg_voice"),
            ).otherwise(0),
        )
        .withColumn(
            "rev_voice_tot_6_10d",
            f.when(
                f.dayofmonth(f.col("event_date")).between(6, 10),
                f.col("rev_payu_voice") + f.col("rev_pkg_voice"),
            ),
        )
        .withColumn(
            "rev_voice_tot_11_15d",
            f.when(
                f.dayofmonth(f.col("event_date")).between(11, 15),
                f.col("rev_payu_voice") + f.col("rev_pkg_voice"),
            ).otherwise(0),
        )
        .withColumn(
            "rev_voice_tot_16_20d",
            f.when(
                f.dayofmonth(f.col("event_date")).between(16, 20),
                f.col("rev_payu_voice") + f.col("rev_pkg_voice"),
            ).otherwise(0),
        )
        .withColumn(
            "rev_voice_tot_21_25d",
            f.when(
                f.dayofmonth(f.col("event_date")).between(21, 25),
                f.col("rev_payu_voice") + f.col("rev_pkg_voice"),
            ).otherwise(0),
        )
        .withColumn(
            "rev_voice_tot_26_31d",
            f.when(
                f.dayofmonth(f.col("event_date")).between(26, 31),
                f.col("rev_payu_voice") + f.col("rev_pkg_voice"),
            ).otherwise(0),
        )
        .withColumn(
            "rev_data_tot_1_5d",
            f.when(
                f.dayofmonth(f.col("event_date")).between(1, 5),
                f.col("rev_payu_data") + f.col("rev_pkg_data"),
            ).otherwise(0),
        )
        .withColumn(
            "rev_data_tot_6_10d",
            f.when(
                f.dayofmonth(f.col("event_date")).between(6, 10),
                f.col("rev_payu_data") + f.col("rev_pkg_data"),
            ),
        )
        .withColumn(
            "rev_data_tot_11_15d",
            f.when(
                f.dayofmonth(f.col("event_date")).between(11, 15),
                f.col("rev_payu_data") + f.col("rev_pkg_data"),
            ).otherwise(0),
        )
        .withColumn(
            "rev_data_tot_16_20d",
            f.when(
                f.dayofmonth(f.col("event_date")).between(16, 20),
                f.col("rev_payu_data") + f.col("rev_pkg_data"),
            ).otherwise(0),
        )
        .withColumn(
            "rev_data_tot_21_25d",
            f.when(
                f.dayofmonth(f.col("event_date")).between(21, 25),
                f.col("rev_payu_data") + f.col("rev_pkg_data"),
            ).otherwise(0),
        )
        .withColumn(
            "rev_data_tot_26_31d",
            f.when(
                f.dayofmonth(f.col("event_date")).between(26, 31),
                f.col("rev_payu_data") + f.col("rev_pkg_data"),
            ).otherwise(0),
        )
        .withColumn(
            "rev_sms_tot_1_5d",
            f.when(
                f.dayofmonth(f.col("event_date")).between(1, 5),
                f.col("rev_payu_sms") + f.col("rev_pkg_sms"),
            ).otherwise(0),
        )
        .withColumn(
            "rev_sms_tot_6_10d",
            f.when(
                f.dayofmonth(f.col("event_date")).between(6, 10),
                f.col("rev_payu_sms") + f.col("rev_pkg_sms"),
            ),
        )
        .withColumn(
            "rev_sms_tot_11_15d",
            f.when(
                f.dayofmonth(f.col("event_date")).between(11, 15),
                f.col("rev_payu_sms") + f.col("rev_pkg_sms"),
            ).otherwise(0),
        )
        .withColumn(
            "rev_sms_tot_16_20d",
            f.when(
                f.dayofmonth(f.col("event_date")).between(16, 20),
                f.col("rev_payu_sms") + f.col("rev_pkg_sms"),
            ).otherwise(0),
        )
        .withColumn(
            "rev_sms_tot_21_25d",
            f.when(
                f.dayofmonth(f.col("event_date")).between(21, 25),
                f.col("rev_payu_sms") + f.col("rev_pkg_sms"),
            ).otherwise(0),
        )
        .withColumn(
            "rev_sms_tot_26_31d",
            f.when(
                f.dayofmonth(f.col("event_date")).between(26, 31),
                f.col("rev_payu_sms") + f.col("rev_pkg_sms"),
            ).otherwise(0),
        )
        .withColumn(
            "rev_roam_tot_1_5d",
            f.when(
                f.dayofmonth(f.col("event_date")).between(1, 5),
                f.col("rev_payu_roam") + f.col("rev_pkg_roam"),
            ).otherwise(0),
        )
        .withColumn(
            "rev_roam_tot_6_10d",
            f.when(
                f.dayofmonth(f.col("event_date")).between(6, 10),
                f.col("rev_payu_roam") + f.col("rev_pkg_roam"),
            ),
        )
        .withColumn(
            "rev_roam_tot_11_15d",
            f.when(
                f.dayofmonth(f.col("event_date")).between(11, 15),
                f.col("rev_payu_roam") + f.col("rev_pkg_roam"),
            ).otherwise(0),
        )
        .withColumn(
            "rev_roam_tot_16_20d",
            f.when(
                f.dayofmonth(f.col("event_date")).between(16, 20),
                f.col("rev_payu_roam") + f.col("rev_pkg_roam"),
            ).otherwise(0),
        )
        .withColumn(
            "rev_roam_tot_21_25d",
            f.when(
                f.dayofmonth(f.col("event_date")).between(21, 25),
                f.col("rev_payu_roam") + f.col("rev_pkg_roam"),
            ).otherwise(0),
        )
        .withColumn(
            "rev_roam_tot_26_31d",
            f.when(
                f.dayofmonth(f.col("event_date")).between(26, 31),
                f.col("rev_payu_roam") + f.col("rev_pkg_roam"),
            ).otherwise(0),
        )
    )


def create_revenue_weekly(
    df_rev_payu_dd: pyspark.sql.DataFrame,
    df_rev_pkg_dd: pyspark.sql.DataFrame,
    df_rev_rcg_dd: pyspark.sql.DataFrame,
) -> None:
    """
    Creates weekly recharge table by:
        1. Unions rev_payu_dd and rev_pkg_dd
        2. Aggregates tot_trx and tot_amt to daily level for each msisdn
        2. Aggregates tot_trx and tot_amt to weekly level for each msisdn

    Args:
        :param df_rev_payu_dd: rev_payu_dd daily level table
        :param df_rev_pkg_dd: rev_pkg_dd daily level table
        :param df_rev_rcg_dd: rev_rcg_dd daily level table

    """

    conf_catalog = get_config_parameters(config="catalog")

    start_date = get_start_date()
    end_date = get_end_date()

    weekly_agg_catalog = conf_catalog["l2_revenue_weekly_data"]

    load_args_1 = conf_catalog["l1_itdaml_dd_cdr_payu_prchse_all"]["load_args"]
    load_args_2 = conf_catalog["l1_itdaml_dd_cdr_pkg_prchse_all"]["load_args"]
    load_args_3 = conf_catalog["l1_usage_rcg_prchse_abt_dd"]["load_args"]

    save_args = weekly_agg_catalog["save_args"]
    save_args.pop("partitionBy", None)
    file_path = get_file_path(filepath=weekly_agg_catalog["filepath"])
    file_format = weekly_agg_catalog["file_format"]
    partitions = int(weekly_agg_catalog["partitions"])
    log.info(
        "Starting Weekly Aggregation for WeekStart {start_date} to {end_date}".format(
            start_date=(start_date + timedelta(days=7)).strftime("%Y-%m-%d"),
            end_date=(end_date + timedelta(days=1)).strftime("%Y-%m-%d"),
        )
    )
    log.info(f"Load Args 1: {load_args_1}")
    log.info(f"Load Args 2: {load_args_2}")
    log.info(f"Load Args 3: {load_args_3}")
    log.info(f"File Path: {file_path}")
    log.info(f"File Format: {file_format}")
    log.info(f"Save Args: {save_args}")
    log.info(f"Partitions: {partitions}")

    while start_date < end_date:
        week_start = (start_date + timedelta(days=7)).strftime("%Y-%m-%d")
        sdate_1 = start_date.strftime(load_args_1["partition_date_format"])
        edate_1 = (start_date + timedelta(days=6)).strftime(
            load_args_1["partition_date_format"]
        )

        sdate_2 = start_date.strftime(load_args_2["partition_date_format"])
        edate_2 = (start_date + timedelta(days=6)).strftime(
            load_args_2["partition_date_format"]
        )

        sdate_3 = start_date.strftime(load_args_3["partition_date_format"])
        edate_3 = (start_date + timedelta(days=6)).strftime(
            load_args_3["partition_date_format"]
        )

        log.info("Starting Weekly Aggregation for WeekStart: {}".format(week_start))

        df_data_1 = df_rev_payu_dd.filter(
            f.col(load_args_1["partition_column"]).between(sdate_1, edate_1)
        ).repartition(1000)

        df_data_2 = df_rev_pkg_dd.filter(
            f.col(load_args_2["partition_column"]).between(sdate_2, edate_2)
        ).repartition(1000)

        df_data_3 = df_rev_rcg_dd.filter(
            f.col(load_args_3["partition_column"]).between(sdate_3, edate_3)
        ).repartition(1000)

        df = _weekly_aggregation(
            df_rev_payu_dd=df_data_1, df_rev_pkg_dd=df_data_2, df_rev_rcg_dd=df_data_3,
        ).drop(f.col("weekstart"))

        partition_file_path = "{file_path}/weekstart={weekstart}".format(
            file_path=file_path, weekstart=week_start
        )

        df.repartition(numPartitions=partitions).write.save(
            partition_file_path, file_format, **save_args
        )
        log.info(
            "Completed Weekly Aggregation for WeekStart: {}".format(
                (start_date + timedelta(days=7)).strftime("%Y-%m-%d")
            )
        )

        start_date += timedelta(days=7)
