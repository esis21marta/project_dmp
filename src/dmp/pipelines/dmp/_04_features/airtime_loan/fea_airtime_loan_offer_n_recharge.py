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


def fea_airtime_loan_offer_n_recharge(
    df_loan: pyspark.sql.DataFrame,
    df_rech: pyspark.sql.DataFrame,
    config_feature: dict,
    feature_mode: str,
    required_output_features: List[str],
) -> pyspark.sql.DataFrame:
    """
    Calculates airtime loan pattern for each msisdn:

    :param df_loan: CMS Tracking Data
    :param df_rech: Recharge Data
    :param config_feature: Features Configurations from Parameters
    :param feature_mode: Features Mode from Parameters [all | list]
    :param required_output_features: Features List from Parameters

    :return: df_fea: Airtime Loan Features

    """

    config_feature.get("airtime", None)

    df_fea_1 = _fea_airtime_loan_offer_triggered(df_loan=df_loan, df_rech=df_rech)
    df_fea_2 = _fea_airtime_loan_rech_time(df_loan=df_loan, df_rech=df_rech)

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


def _fea_airtime_loan_offer_triggered(
    df_loan: pyspark.sql.DataFrame, df_rech: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:
    """
    Calculates airtime loan pattern for each msisdn:

    Args:
        df_loan: CMS Tracking Data
        df_rech: Recharge Data

    Returns:
        df_fea: Airtime Loan Features

    """

    df = (
        df_rech.join(df_loan, ["msisdn", "weekstart"], how="full")
        .withColumn(
            "fea_airtime_loan_offer_triggered_1w",
            f.size(
                f.array_distinct(
                    f.flatten(
                        f.collect_set(
                            f.when(
                                (f.col("account_balance_min") <= 500),
                                f.col("loan_offers_sent"),
                            )
                        ).over(get_rolling_window(7 * 1, "msisdn", "weekstart"))
                    )
                )
            ),
        )
        .withColumn(
            "fea_airtime_loan_offer_triggered_2w",
            f.size(
                f.array_distinct(
                    f.flatten(
                        f.collect_set(
                            f.when(
                                (f.col("account_balance_min") <= 500),
                                f.col("loan_offers_sent"),
                            )
                        ).over(get_rolling_window(7 * 2, "msisdn", "weekstart"))
                    )
                )
            ),
        )
        .withColumn(
            "fea_airtime_loan_offer_triggered_3w",
            f.size(
                f.array_distinct(
                    f.flatten(
                        f.collect_set(
                            f.when(
                                (f.col("account_balance_min") <= 500),
                                f.col("loan_offers_sent"),
                            )
                        ).over(get_rolling_window(7 * 3, "msisdn", "weekstart"))
                    )
                )
            ),
        )
        .withColumn(
            "fea_airtime_loan_offer_triggered_1m",
            f.size(
                f.array_distinct(
                    f.flatten(
                        f.collect_set(
                            f.when(
                                (f.col("account_balance_min") <= 500),
                                f.col("loan_offers_sent"),
                            )
                        ).over(get_rolling_window(7 * 4, "msisdn", "weekstart"))
                    )
                )
            ),
        )
        .withColumn(
            "fea_airtime_loan_offer_triggered_2m",
            f.size(
                f.array_distinct(
                    f.flatten(
                        f.collect_set(
                            f.when(
                                (f.col("account_balance_min") <= 500),
                                f.col("loan_offers_sent"),
                            )
                        ).over(get_rolling_window(7 * 8, "msisdn", "weekstart"))
                    )
                )
            ),
        )
        .withColumn(
            "fea_airtime_loan_offer_triggered_3m",
            f.size(
                f.array_distinct(
                    f.flatten(
                        f.collect_set(
                            f.when(
                                (f.col("account_balance_min") <= 500),
                                f.col("loan_offers_sent"),
                            )
                        ).over(get_rolling_window(7 * 13, "msisdn", "weekstart"))
                    )
                )
            ),
        )
        .select(
            "msisdn",
            "weekstart",
            "fea_airtime_loan_offer_triggered_1w",
            "fea_airtime_loan_offer_triggered_2w",
            "fea_airtime_loan_offer_triggered_3w",
            "fea_airtime_loan_offer_triggered_1m",
            "fea_airtime_loan_offer_triggered_2m",
            "fea_airtime_loan_offer_triggered_3m",
        )
    )

    return df


def _fea_airtime_loan_rech_time(
    df_loan: pyspark.sql.DataFrame, df_rech: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:
    """
    Calculates airtime loan pattern for each msisdn:

    Args:
        df_loan: CMS Tracking Data
        df_rech: Recharge Data

    Returns:
        df_fea: Airtime Loan Features

    """

    df_rech = df_rech.withColumn("recharge_date", f.explode(f.col("recharge_date")))
    df_loan_acc = (
        df_loan.withColumn(
            "loan_offers_accepted", f.explode(f.col("loan_offers_accepted_with_dates"))
        )
        .withColumn(
            "loan_offers_accepted_event_date",
            f.col("loan_offers_accepted")["event_date"],
        )
        .withColumn("campaign_id", f.col("loan_offers_accepted")["campaignid"])
        .withColumn(
            "campaign_id", f.regexp_extract(f.col("campaign_id"), "(.*)-LOAN-(.*)", 1)
        )
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
        .withColumn(
            "campaign_id", f.regexp_extract(f.col("campaign_id"), "(.*)-LOAN-(.*)", 1)
        )
        .select("msisdn", "weekstart", "loan_offers_sent_event_date", "campaign_id")
    )

    df_scaffold = df_loan.select("msisdn", "weekstart")

    df_loan = (
        df_loan_sent.join(df_loan_acc, ["msisdn", "campaign_id"], how="left")
        .select(
            "msisdn",
            df_loan_sent["weekstart"],
            "loan_offers_sent_event_date",
            "loan_offers_accepted_event_date",
        )
        .groupBy("msisdn", "weekstart")
        .agg(
            f.min("loan_offers_sent_event_date").alias("loan_offers_sent_event_date"),
            f.min(
                f.when(
                    f.col("loan_offers_sent_event_date")
                    <= f.col("loan_offers_accepted_event_date"),
                    f.col("loan_offers_accepted_event_date"),
                )
            ).alias("loan_offers_accepted_event_date"),
        )
    )

    df = (
        df_loan.join(df_rech, ["msisdn"], how="left")
        .filter(f.col("loan_offers_sent_event_date") <= f.col("recharge_date"))
        .select(
            "msisdn",
            df_loan_sent["weekstart"],
            "loan_offers_sent_event_date",
            "loan_offers_accepted_event_date",
            "recharge_date",
        )
        .groupBy("msisdn", "weekstart",)
        .agg(
            f.min("loan_offers_sent_event_date").alias("loan_offers_sent_event_date"),
            f.min("loan_offers_accepted_event_date").alias(
                "loan_offers_accepted_event_date"
            ),
            f.min("recharge_date").alias("recharge_date"),
        )
    )

    df_fea = (
        df_scaffold.join(df, ["msisdn", "weekstart"], how="left")
        .withColumn(
            "fea_airtime_loan_offer_rech_gap_without_takeup_1w",
            f.when(
                (f.col("loan_offers_accepted_event_date").isNull())
                | (f.col("loan_offers_accepted_event_date") > f.col("recharge_date")),
                f.datediff(
                    f.col("recharge_date"), f.col("loan_offers_sent_event_date")
                ),
            ),
        )
        .withColumn(
            "fea_airtime_loan_offer_rech_gap_without_takeup_2w",
            f.avg(f.col("fea_airtime_loan_offer_rech_gap_without_takeup_1w")).over(
                get_rolling_window(7 * 2, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_airtime_loan_offer_rech_gap_without_takeup_3w",
            f.avg(f.col("fea_airtime_loan_offer_rech_gap_without_takeup_1w")).over(
                get_rolling_window(7 * 3, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_airtime_loan_offer_rech_gap_without_takeup_1m",
            f.avg(f.col("fea_airtime_loan_offer_rech_gap_without_takeup_1w")).over(
                get_rolling_window(7 * 4, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_airtime_loan_offer_rech_gap_without_takeup_2m",
            f.avg(f.col("fea_airtime_loan_offer_rech_gap_without_takeup_1w")).over(
                get_rolling_window(7 * 8, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_airtime_loan_offer_rech_gap_without_takeup_3m",
            f.avg(f.col("fea_airtime_loan_offer_rech_gap_without_takeup_1w")).over(
                get_rolling_window(7 * 13, "msisdn", "weekstart")
            ),
        )
        .select(
            "msisdn",
            "weekstart",
            "fea_airtime_loan_offer_rech_gap_without_takeup_1w",
            "fea_airtime_loan_offer_rech_gap_without_takeup_2w",
            "fea_airtime_loan_offer_rech_gap_without_takeup_3w",
            "fea_airtime_loan_offer_rech_gap_without_takeup_1m",
            "fea_airtime_loan_offer_rech_gap_without_takeup_2m",
            "fea_airtime_loan_offer_rech_gap_without_takeup_3m",
        )
    )

    return df_fea
