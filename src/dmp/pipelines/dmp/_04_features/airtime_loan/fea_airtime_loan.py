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
from pyspark.sql import functions as f

from utils import (
    get_end_date,
    get_required_output_columns,
    get_rolling_window,
    get_start_date,
)


def fea_airtime_loan(
    df_loan: pyspark.sql.DataFrame,
    config_feature: dict,
    feature_mode: str,
    required_output_features: List[str],
) -> pyspark.sql.DataFrame:
    """
    Calculates airtime loan pattern for each msisdn:

    :param df_loan: CMS Tracking Data
    :param config_feature: Features Configurations from Parameters
    :param feature_mode: Features Mode from Parameters [all | list]
    :param required_output_features: Features List from Parameters

    :return: df_fea: Airtime Loan Features
    """

    config_feature.get("airtime", None)

    df_fea_1 = _fea_airtime_loan_tracking(df_loan=df_loan)
    df_fea_2 = _fea_airtime_loan_offer_takeup(df_loan=df_loan)

    df_fea = df_fea_1.join(df_fea_2, ["msisdn", "weekstart"], how="full")

    dimension_columns = ["msisdn", "weekstart"]

    features_columns = list(set(df_fea.schema.names) - set(dimension_columns))

    required_output_columns = get_required_output_columns(
        output_features=features_columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=dimension_columns,
    )

    df_fea = df_fea.select(required_output_columns)

    first_week_start = get_start_date(partition_column="weekstart").strftime("%Y-%m-%d")
    last_week_start = get_end_date(partition_column="weekstart").strftime("%Y-%m-%d")

    return df_fea.filter(f.col("weekstart").between(first_week_start, last_week_start))


def _fea_airtime_loan_tracking(df_loan: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Calculates airtime loan pattern for each msisdn:

    Args:
        df_loan: CMS Tracking Data

    Returns:
        df_fea: Airtime Loan Features

    """

    df_fea = (
        df_loan.withColumn(
            "fea_airtime_loan_offers_sent_1w",
            f.size(
                f.array_distinct(
                    f.flatten(
                        f.collect_set(f.col("loan_offers_sent")).over(
                            get_rolling_window(7 * 1, "msisdn", "weekstart")
                        )
                    )
                )
            ),
        )
        .withColumn(
            "fea_airtime_loan_offers_sent_2w",
            f.size(
                f.array_distinct(
                    f.flatten(
                        f.collect_set(f.col("loan_offers_sent")).over(
                            get_rolling_window(7 * 2, "msisdn", "weekstart")
                        )
                    )
                )
            ),
        )
        .withColumn(
            "fea_airtime_loan_offers_sent_3w",
            f.size(
                f.array_distinct(
                    f.flatten(
                        f.collect_set(f.col("loan_offers_sent")).over(
                            get_rolling_window(7 * 3, "msisdn", "weekstart")
                        )
                    )
                )
            ),
        )
        .withColumn(
            "fea_airtime_loan_offers_sent_1m",
            f.size(
                f.array_distinct(
                    f.flatten(
                        f.collect_set(f.col("loan_offers_sent")).over(
                            get_rolling_window(7 * 4, "msisdn", "weekstart")
                        )
                    )
                )
            ),
        )
        .withColumn(
            "fea_airtime_loan_offers_sent_2m",
            f.size(
                f.array_distinct(
                    f.flatten(
                        f.collect_set(f.col("loan_offers_sent")).over(
                            get_rolling_window(7 * 8, "msisdn", "weekstart")
                        )
                    )
                )
            ),
        )
        .withColumn(
            "fea_airtime_loan_offers_sent_3m",
            f.size(
                f.array_distinct(
                    f.flatten(
                        f.collect_set(f.col("loan_offers_sent")).over(
                            get_rolling_window(7 * 13, "msisdn", "weekstart")
                        )
                    )
                )
            ),
        )
        .withColumn(
            "fea_airtime_loan_offers_notifications_1w",
            f.sum(f.col("loan_offers_notifications")).over(
                get_rolling_window(7 * 1, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_airtime_loan_offers_notifications_2w",
            f.sum(f.col("loan_offers_notifications")).over(
                get_rolling_window(7 * 2, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_airtime_loan_offers_notifications_3w",
            f.sum(f.col("loan_offers_notifications")).over(
                get_rolling_window(7 * 3, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_airtime_loan_offers_notifications_1m",
            f.sum(f.col("loan_offers_notifications")).over(
                get_rolling_window(7 * 4, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_airtime_loan_offers_notifications_2m",
            f.sum(f.col("loan_offers_notifications")).over(
                get_rolling_window(7 * 8, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_airtime_loan_offers_notifications_3m",
            f.sum(f.col("loan_offers_notifications")).over(
                get_rolling_window(7 * 13, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_airtime_loan_offers_accepted_1w",
            f.size(
                f.array_distinct(
                    f.flatten(
                        f.collect_set(f.col("loan_offers_accepted")).over(
                            get_rolling_window(7 * 1, "msisdn", "weekstart")
                        )
                    )
                )
            ),
        )
        .withColumn(
            "fea_airtime_loan_offers_accepted_2w",
            f.size(
                f.array_distinct(
                    f.flatten(
                        f.collect_set(f.col("loan_offers_accepted")).over(
                            get_rolling_window(7 * 2, "msisdn", "weekstart")
                        )
                    )
                )
            ),
        )
        .withColumn(
            "fea_airtime_loan_offers_accepted_3w",
            f.size(
                f.array_distinct(
                    f.flatten(
                        f.collect_set(f.col("loan_offers_accepted")).over(
                            get_rolling_window(7 * 3, "msisdn", "weekstart")
                        )
                    )
                )
            ),
        )
        .withColumn(
            "fea_airtime_loan_offers_accepted_1m",
            f.size(
                f.array_distinct(
                    f.flatten(
                        f.collect_set(f.col("loan_offers_accepted")).over(
                            get_rolling_window(7 * 4, "msisdn", "weekstart")
                        )
                    )
                )
            ),
        )
        .withColumn(
            "fea_airtime_loan_offers_accepted_2m",
            f.size(
                f.array_distinct(
                    f.flatten(
                        f.collect_set(f.col("loan_offers_accepted")).over(
                            get_rolling_window(7 * 8, "msisdn", "weekstart")
                        )
                    )
                )
            ),
        )
        .withColumn(
            "fea_airtime_loan_offers_accepted_3m",
            f.size(
                f.array_distinct(
                    f.flatten(
                        f.collect_set(f.col("loan_offers_accepted")).over(
                            get_rolling_window(7 * 13, "msisdn", "weekstart")
                        )
                    )
                )
            ),
        )
        .withColumn(
            "fea_airtime_loan_offers_repaid_1w",
            f.sum(f.col("loan_offers_repaid")).over(
                get_rolling_window(7 * 1, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_airtime_loan_offers_repaid_2w",
            f.sum(f.col("loan_offers_repaid")).over(
                get_rolling_window(7 * 2, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_airtime_loan_offers_repaid_3w",
            f.sum(f.col("loan_offers_repaid")).over(
                get_rolling_window(7 * 3, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_airtime_loan_offers_repaid_1m",
            f.sum(f.col("loan_offers_repaid")).over(
                get_rolling_window(7 * 4, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_airtime_loan_offers_repaid_2m",
            f.sum(f.col("loan_offers_repaid")).over(
                get_rolling_window(7 * 8, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_airtime_loan_offers_repaid_3m",
            f.sum(f.col("loan_offers_repaid")).over(
                get_rolling_window(7 * 13, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_airtime_loan_offers_eligible_1w",
            f.sum(f.col("loan_offers_eligible")).over(
                get_rolling_window(7 * 1, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_airtime_loan_offers_eligible_2w",
            f.sum(f.col("loan_offers_eligible")).over(
                get_rolling_window(7 * 2, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_airtime_loan_offers_eligible_3w",
            f.sum(f.col("loan_offers_eligible")).over(
                get_rolling_window(7 * 3, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_airtime_loan_offers_eligible_1m",
            f.sum(f.col("loan_offers_eligible")).over(
                get_rolling_window(7 * 4, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_airtime_loan_offers_eligible_2m",
            f.sum(f.col("loan_offers_eligible")).over(
                get_rolling_window(7 * 8, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_airtime_loan_offers_eligible_3m",
            f.sum(f.col("loan_offers_eligible")).over(
                get_rolling_window(7 * 13, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_airtime_whitelisted_1w",
            f.when(f.col("loan_offers_eligible") > 0, 1).otherwise(0),
        )
        .withColumnRenamed(
            "avg_eligible_loan_amount",
            "fea_airtime_loan_offers_eligible_amount_avg_1w",
        )
    )

    df_fea = (
        df_fea.withColumn(
            "fea_airtime_loan_offers_acceptance_ratio_1w",
            f.col("fea_airtime_loan_offers_accepted_1w")
            / f.col("fea_airtime_loan_offers_sent_1w"),
        )
        .withColumn(
            "fea_airtime_loan_offers_acceptance_ratio_2w",
            f.col("fea_airtime_loan_offers_accepted_2w")
            / f.col("fea_airtime_loan_offers_sent_2w"),
        )
        .withColumn(
            "fea_airtime_loan_offers_acceptance_ratio_3w",
            f.col("fea_airtime_loan_offers_accepted_3w")
            / f.col("fea_airtime_loan_offers_sent_3w"),
        )
        .withColumn(
            "fea_airtime_loan_offers_acceptance_ratio_1m",
            f.col("fea_airtime_loan_offers_accepted_1m")
            / f.col("fea_airtime_loan_offers_sent_1m"),
        )
        .withColumn(
            "fea_airtime_loan_offers_acceptance_ratio_2m",
            f.col("fea_airtime_loan_offers_accepted_2m")
            / f.col("fea_airtime_loan_offers_sent_2m"),
        )
        .withColumn(
            "fea_airtime_loan_offers_acceptance_ratio_3m",
            f.col("fea_airtime_loan_offers_accepted_3m")
            / f.col("fea_airtime_loan_offers_sent_3m"),
        )
    )

    df_fea = df_fea.select(
        "msisdn",
        "weekstart",
        "fea_airtime_loan_offers_sent_1w",
        "fea_airtime_loan_offers_sent_2w",
        "fea_airtime_loan_offers_sent_3w",
        "fea_airtime_loan_offers_sent_1m",
        "fea_airtime_loan_offers_sent_2m",
        "fea_airtime_loan_offers_sent_3m",
        "fea_airtime_loan_offers_notifications_1w",
        "fea_airtime_loan_offers_notifications_2w",
        "fea_airtime_loan_offers_notifications_3w",
        "fea_airtime_loan_offers_notifications_1m",
        "fea_airtime_loan_offers_notifications_2m",
        "fea_airtime_loan_offers_notifications_3m",
        "fea_airtime_loan_offers_accepted_1w",
        "fea_airtime_loan_offers_accepted_2w",
        "fea_airtime_loan_offers_accepted_3w",
        "fea_airtime_loan_offers_accepted_1m",
        "fea_airtime_loan_offers_accepted_2m",
        "fea_airtime_loan_offers_accepted_3m",
        "fea_airtime_loan_offers_repaid_1w",
        "fea_airtime_loan_offers_repaid_2w",
        "fea_airtime_loan_offers_repaid_3w",
        "fea_airtime_loan_offers_repaid_1m",
        "fea_airtime_loan_offers_repaid_2m",
        "fea_airtime_loan_offers_repaid_3m",
        "fea_airtime_loan_offers_eligible_1w",
        "fea_airtime_loan_offers_eligible_2w",
        "fea_airtime_loan_offers_eligible_3w",
        "fea_airtime_loan_offers_eligible_1m",
        "fea_airtime_loan_offers_eligible_2m",
        "fea_airtime_loan_offers_eligible_3m",
        "fea_airtime_whitelisted_1w",
        "fea_airtime_loan_offers_eligible_amount_avg_1w",
        "fea_airtime_loan_offers_acceptance_ratio_1w",
        "fea_airtime_loan_offers_acceptance_ratio_2w",
        "fea_airtime_loan_offers_acceptance_ratio_3w",
        "fea_airtime_loan_offers_acceptance_ratio_1m",
        "fea_airtime_loan_offers_acceptance_ratio_2m",
        "fea_airtime_loan_offers_acceptance_ratio_3m",
    )

    return df_fea


def _fea_airtime_loan_offer_takeup(
    df_loan: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    Calculates airtime loan pattern for each msisdn:

    Args:
        df_loan: CMS Tracking Data

    Returns:
        df_fea: Airtime Loan Features

    """
    df_loan_acc = (
        df_loan.withColumn(
            "loan_offers_accepted", f.explode(f.col("loan_offers_accepted_with_dates"))
        )
        .withColumn(
            "loan_offers_accepted_event_date",
            f.col("loan_offers_accepted")["event_date"],
        )
        .withColumn("campaign_id", f.col("loan_offers_accepted")["campaignid"],)
        .select(
            "msisdn", "weekstart", "loan_offers_accepted_event_date", "campaign_id",
        )
    )

    df_loan_sent = (
        df_loan.withColumn(
            "loan_offers_sent", f.explode(f.col("loan_offers_sent_with_dates"))
        )
        .withColumn(
            "loan_offers_sent_event_date", f.col("loan_offers_sent")["event_date"],
        )
        .withColumn("campaign_id", f.col("loan_offers_sent")["campaignid"],)
        .select("msisdn", "weekstart", "loan_offers_sent_event_date", "campaign_id")
    )

    df_scaffold = df_loan.select("msisdn", "weekstart")

    df = df_loan_acc.join(df_loan_sent, ["msisdn", "campaign_id"], how="left").select(
        "msisdn",
        df_loan_acc["weekstart"],
        "loan_offers_accepted_event_date",
        "loan_offers_sent_event_date",
    )

    df = df.groupBy("msisdn", "weekstart").agg(
        f.max(
            f.datediff("loan_offers_accepted_event_date", "loan_offers_sent_event_date")
        ).alias("fea_airtime_loan_avg_takeup_time_1w")
    )

    df_fea = (
        df_scaffold.join(df, ["msisdn", "weekstart"], how="left")
        .withColumn(
            "fea_airtime_loan_avg_takeup_time_2w",
            f.avg(f.col("fea_airtime_loan_avg_takeup_time_1w")).over(
                get_rolling_window(7 * 2, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_airtime_loan_avg_takeup_time_3w",
            f.avg(f.col("fea_airtime_loan_avg_takeup_time_1w")).over(
                get_rolling_window(7 * 3, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_airtime_loan_avg_takeup_time_1m",
            f.avg(f.col("fea_airtime_loan_avg_takeup_time_1w")).over(
                get_rolling_window(7 * 4, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_airtime_loan_avg_takeup_time_2m",
            f.avg(f.col("fea_airtime_loan_avg_takeup_time_1w")).over(
                get_rolling_window(7 * 8, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_airtime_loan_avg_takeup_time_3m",
            f.avg(f.col("fea_airtime_loan_avg_takeup_time_1w")).over(
                get_rolling_window(7 * 13, "msisdn", "weekstart")
            ),
        )
        .select(
            "msisdn",
            "weekstart",
            "fea_airtime_loan_avg_takeup_time_1w",
            "fea_airtime_loan_avg_takeup_time_2w",
            "fea_airtime_loan_avg_takeup_time_3w",
            "fea_airtime_loan_avg_takeup_time_1m",
            "fea_airtime_loan_avg_takeup_time_2m",
            "fea_airtime_loan_avg_takeup_time_3m",
        )
    )

    return df_fea
