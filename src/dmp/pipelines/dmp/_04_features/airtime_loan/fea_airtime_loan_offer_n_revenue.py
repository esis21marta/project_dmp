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
from pyspark.sql.window import Window

from utils import (
    get_end_date,
    get_required_output_columns,
    get_rolling_window,
    get_start_date,
)


def fea_airtime_loan_offer_n_revenue(
    df_loan: pyspark.sql.DataFrame,
    df_rev: pyspark.sql.DataFrame,
    config_feature: dict,
    feature_mode: str,
    required_output_features: List[str],
) -> pyspark.sql.DataFrame:
    """
    Calculates airtime loan pattern for each msisdn:

    :param df_loan: CMS Tracking Data
    :param df_rev: Revenue Data
    :param config_feature: Features Configurations from Parameters
    :param feature_mode: Features Mode from Parameters [all | list]
    :param required_output_features: Features List from Parameters

    :return: df_fea: Airtime Loan Features

    """

    config_feature.get("airtime", None)

    df_fea_1 = _fea_airtime_loan_repayment(df_loan=df_loan, df_rev=df_rev)
    df_fea_2 = _fea_airtime_multiple_loan_without_repayment(
        df_loan=df_loan, df_rev=df_rev
    )

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


def _fea_airtime_loan_repayment(
    df_loan: pyspark.sql.DataFrame, df_rev: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:
    """
    Calculates airtime loan pattern for each msisdn:

    Args:
        df_loan: CMS Tracking Data
        df_rev: Revenue Data

    Returns:
        df_fea: Airtime Loan Features

    """
    df_loan = df_loan.select("msisdn", "weekstart", "loan_date")
    df_loan_date = df_loan.withColumn(
        "loan_date", f.explode(f.col("loan_date"))
    ).withColumn(
        "prev_loan_date",
        f.lag(f.col("loan_date")).over(
            Window.partitionBy("msisdn").orderBy(f.col("loan_date"))
        ),
    )

    df_rev = df_rev.select("msisdn", "weekstart", "loan_repayment_date")
    df_rev_date = df_rev.withColumn(
        "loan_repayment_date", f.explode(f.col("loan_repayment_date"))
    ).withColumn(
        "prev_loan_repayment_date",
        f.lag(f.col("loan_repayment_date")).over(
            Window.partitionBy("msisdn").orderBy(f.col("loan_repayment_date"))
        ),
    )

    df_scaffold = df_rev.select("msisdn", "weekstart")

    df_loan_repayment = (
        df_rev_date.join(df_loan_date, ["msisdn"], how="left")
        .filter(f.col("loan_date") <= f.col("loan_repayment_date"))
        .withColumn(
            "rank",
            f.row_number().over(
                Window.partitionBy("msisdn", "loan_repayment_date").orderBy(
                    f.col("loan_date").desc()
                )
            ),
        )
        .select(
            "msisdn",
            df_rev_date["weekstart"],
            "loan_repayment_date",
            "prev_loan_repayment_date",
            "loan_date",
            "rank",
        )
    )

    df_loan_repayment_1 = df_loan_repayment.filter(
        f.col("prev_loan_repayment_date").isNull()
    )
    df_loan_repayment_2 = df_loan_repayment.filter(
        f.col("prev_loan_repayment_date").isNotNull()
    )

    df_loan_repayment_1 = df_loan_repayment_1.groupBy(
        "msisdn", "weekstart", "loan_repayment_date"
    ).agg(f.min("loan_date").alias("actual_loan_date"))

    df_loan_repayment_2 = df_loan_repayment_2.groupBy(
        "msisdn", "weekstart", "loan_repayment_date"
    ).agg(
        f.coalesce(
            f.min(
                f.when(
                    f.col("loan_date") > f.col("prev_loan_repayment_date"),
                    f.col("loan_date"),
                )
            ),
            f.min(f.when(f.col("rank") == 1, f.col("loan_date"))),
        ).alias("actual_loan_date")
    )

    df_loan_repayment = df_loan_repayment_1.union(df_loan_repayment_2).withColumn(
        "repayment_duration",
        f.datediff(f.col("loan_repayment_date"), f.col("actual_loan_date")),
    )

    df_loan_repayment = df_scaffold.join(
        df_loan_repayment, ["msisdn", "weekstart"], how="left"
    )

    df = (
        df_loan_repayment.withColumn(
            "fea_airtime_loan_avg_repayment_duration_1m",
            f.avg(f.col("repayment_duration")).over(
                get_rolling_window(7 * 4, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_airtime_loan_avg_repayment_duration_2m",
            f.avg(f.col("repayment_duration")).over(
                get_rolling_window(7 * 8, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_airtime_loan_avg_repayment_duration_3m",
            f.avg(f.col("repayment_duration")).over(
                get_rolling_window(7 * 13, "msisdn", "weekstart")
            ),
        )
        .select(
            "msisdn",
            "weekstart",
            "fea_airtime_loan_avg_repayment_duration_1m",
            "fea_airtime_loan_avg_repayment_duration_2m",
            "fea_airtime_loan_avg_repayment_duration_3m",
        )
    )

    return df


def _fea_airtime_multiple_loan_without_repayment(
    df_loan: pyspark.sql.DataFrame, df_rev: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:
    """
    Calculates airtime loan pattern for each msisdn:

    Args:
        df_loan: CMS Tracking Data
        df_rev: Revenue Data

    Returns:
        df_fea: Airtime Loan Features

    """
    df_loan = df_loan.select("msisdn", "weekstart", "loan_date")
    df_loan_date = df_loan.withColumn(
        "loan_date", f.explode(f.col("loan_date"))
    ).withColumn(
        "prev_loan_date",
        f.lag(f.col("loan_date")).over(
            Window.partitionBy("msisdn").orderBy(f.col("loan_date"))
        ),
    )

    df_rev = df_rev.select("msisdn", "weekstart", "loan_repayment_date")
    df_rev_date = df_rev.withColumn(
        "loan_repayment_date", f.explode(f.col("loan_repayment_date"))
    ).withColumn(
        "prev_loan_repayment_date",
        f.lag(f.col("loan_repayment_date")).over(
            Window.partitionBy("msisdn").orderBy(f.col("loan_repayment_date"))
        ),
    )

    df_scaffold = df_rev.select("msisdn", "weekstart")

    df_loan_repayment = (
        df_loan_date.join(df_rev_date, ["msisdn"], how="left")
        .filter(
            (f.col("loan_date") > f.col("loan_repayment_date"))
            & (
                (f.col("prev_loan_date") <= f.col("loan_repayment_date"))
                | (f.col("prev_loan_date").isNull())
            )
        )
        .select(
            "msisdn",
            df_loan_date["weekstart"],
            "loan_date",
            "prev_loan_date",
            df_rev_date["loan_repayment_date"].alias("prev_loan_repayment_date"),
        )
    )

    df_loan_repayment = df_scaffold.join(
        df_loan_repayment, ["msisdn", "weekstart"], how="left"
    )

    df_fea = df_loan_repayment.groupBy("msisdn", "weekstart").agg(
        f.count(
            f.when(
                f.col("loan_date").isNotNull()
                & f.col("prev_loan_date").isNotNull()
                & f.col("prev_loan_repayment_date").isNull(),
                f.col("loan_date"),
            )
        ).alias("fea_airtime_loan_count_without_payment_1w")
    )

    df_fea = (
        df_fea.withColumn(
            "fea_airtime_loan_count_without_payment_2w",
            f.sum("fea_airtime_loan_count_without_payment_1w").over(
                get_rolling_window(7 * 2, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_airtime_loan_count_without_payment_3w",
            f.sum("fea_airtime_loan_count_without_payment_1w").over(
                get_rolling_window(7 * 3, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_airtime_loan_count_without_payment_1m",
            f.sum("fea_airtime_loan_count_without_payment_1w").over(
                get_rolling_window(7 * 4, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_airtime_loan_count_without_payment_2m",
            f.sum("fea_airtime_loan_count_without_payment_1w").over(
                get_rolling_window(7 * 8, "msisdn", "weekstart")
            ),
        )
        .withColumn(
            "fea_airtime_loan_count_without_payment_3m",
            f.sum("fea_airtime_loan_count_without_payment_1w").over(
                get_rolling_window(7 * 13, "msisdn", "weekstart")
            ),
        )
        .select(
            "msisdn",
            "weekstart",
            "fea_airtime_loan_count_without_payment_1w",
            "fea_airtime_loan_count_without_payment_2w",
            "fea_airtime_loan_count_without_payment_3w",
            "fea_airtime_loan_count_without_payment_1m",
            "fea_airtime_loan_count_without_payment_2m",
            "fea_airtime_loan_count_without_payment_3m",
        )
    )

    return df_fea
