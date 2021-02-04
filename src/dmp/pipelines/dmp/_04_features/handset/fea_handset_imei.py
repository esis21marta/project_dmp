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

from utils import (
    get_end_date,
    get_month_id_bw_sd_ed,
    get_required_output_columns,
    get_start_date,
)


def fea_handset_imei(
    df_handset_fea: pyspark.sql.DataFrame,
    df_imei_user_count: pyspark.sql.DataFrame,
    sla_date_parameter,
    feature_mode: str,
    required_output_features: List[str],
) -> pyspark.sql.DataFrame:
    """
    create feature for handset from weekly data

    Args:
        df_handset_fea: Handset Weekly Data
        df_imei_user_count: Data  containing count of unique msisdns per imei at monthly level

    Returns:
        fea_df: Handset Features
    """

    df_handset = (
        df_handset_fea.withColumn("imei", f.explode(f.col("imeis")))
        .groupBy("msisdn", "weekstart")
        .agg(f.last("imei").alias("imei"))
    )

    first_week_start = get_start_date()
    last_week_start = get_end_date()
    df_month = get_month_id_bw_sd_ed(
        first_week_start, last_week_start, sla_date_parameter
    )

    df_month = df_month.withColumnRenamed("mo_id", "month")

    df_imei_user_count = df_imei_user_count.withColumn(
        "month", f.date_format(f.to_date(f.col("month"), "yyyyMM"), "yyyy-MM")
    )

    df_imei_user_count = df_imei_user_count.join(df_month, on=["month"])

    df_handset = (
        df_handset.join(df_imei_user_count, on=["weekstart", "imei"], how="left")
        .withColumnRenamed("uniq_msisdn_6_month", "fea_handset_uniq_msisdn_06m")
        .withColumnRenamed("uniq_msisdn_3_month", "fea_handset_uniq_msisdn_03m")
        .withColumnRenamed("uniq_msisdn_1_month", "fea_handset_uniq_msisdn_01m")
    )

    output_features_columns = [
        "fea_handset_uniq_msisdn_06m",
        "fea_handset_uniq_msisdn_03m",
        "fea_handset_uniq_msisdn_01m",
    ]

    required_output_columns = get_required_output_columns(
        output_features=output_features_columns,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["msisdn", "weekstart"],
    )

    df_features = df_handset.select(required_output_columns)

    return df_features.filter(
        f.col("weekstart").between(first_week_start, last_week_start)
    )
