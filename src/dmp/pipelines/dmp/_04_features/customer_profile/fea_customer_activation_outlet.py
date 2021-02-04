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
    get_required_output_columns,
    get_rolling_window,
    get_start_date,
)


def fea_customer_activation_outlet(
    sellthru_df: pyspark.sql.DataFrame,
    outlet_dim_df: pyspark.sql.DataFrame,
    feature_mode: str,
    required_output_features: List[str],
) -> pyspark.sql.DataFrame:

    df_data = sellthru_df.join(outlet_dim_df, on=["outlet_id", "weekstart"], how="left")

    df_fea = fea_df = (
        df_data.withColumn(
            f"fea_custprof_activation_outlet_id_06m",
            f.last("outlet_id").over(get_rolling_window(7 * 26, oby="weekstart")),
        )
        .withColumn(
            f"fea_custprof_activation_outlet_name_06m",
            f.last("outlet_name").over(get_rolling_window(7 * 26, oby="weekstart")),
        )
        .withColumn(
            f"fea_custprof_activation_outlet_type_06m",
            f.last("outlet_type").over(get_rolling_window(7 * 26, oby="weekstart")),
        )
        .withColumn(
            f"fea_custprof_activation_latitude_06m",
            f.last("latitude").over(get_rolling_window(7 * 26, oby="weekstart")),
        )
        .withColumn(
            f"fea_custprof_activation_longitude_06m",
            f.last("longitude").over(get_rolling_window(7 * 26, oby="weekstart")),
        )
        .withColumn(
            f"fea_custprof_activation_outlet_location_type_06m",
            f.last("location_type").over(get_rolling_window(7 * 26, oby="weekstart")),
        )
        .withColumn(
            f"fea_custprof_activation_outlet_classification_06m",
            f.last("classification").over(get_rolling_window(7 * 26, oby="weekstart")),
        )
        .withColumn(
            f"fea_custprof_activation_outlet_kecamatan_06m",
            f.last("outlet_kecamatan").over(
                get_rolling_window(7 * 26, oby="weekstart")
            ),
        )
        .withColumn(
            f"fea_custprof_activation_outlet_kabupaten_06m",
            f.last("outlet_kabupaten").over(
                get_rolling_window(7 * 26, oby="weekstart")
            ),
        )
    )

    output_features = [
        "fea_custprof_activation_outlet_id_06m",
        "fea_custprof_activation_outlet_name_06m",
        "fea_custprof_activation_outlet_type_06m",
        "fea_custprof_activation_latitude_06m",
        "fea_custprof_activation_longitude_06m",
        "fea_custprof_activation_outlet_location_type_06m",
        "fea_custprof_activation_outlet_classification_06m",
        "fea_custprof_activation_outlet_kabupaten_06m",
        "fea_custprof_activation_outlet_kecamatan_06m",
    ]

    required_output_columns = get_required_output_columns(
        output_features=output_features,
        feature_mode=feature_mode,
        feature_list=required_output_features,
        extra_columns_to_keep=["msisdn", "weekstart"],
    )

    df_fea = df_fea.select(required_output_columns)
    first_week_start = get_start_date(partition_column="weekstart").strftime("%Y-%m-%d")
    last_week_start = get_end_date(partition_column="weekstart").strftime("%Y-%m-%d")

    return df_fea.filter(f.col("weekstart").between(first_week_start, last_week_start))
