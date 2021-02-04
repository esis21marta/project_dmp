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

import datetime
from unittest import mock

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.dmp.pipelines.dmp._04_features.product.fea_macroproduct import (
    create_product_features,
)
from src.tests.pysaprk_df_equality import assert_df_frame_equal


@pytest.fixture
def product_agg_data():
    data = [
        (
            "AAAAAA",
            "2020-04-06",
            30.0,
            30.0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            1,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            [["data0_dpi0_voice0_sms1_val5to10", "1", "30.0"]],
        ),
        (
            "CCCCCC",
            "2020-03-09",
            365.0,
            365.0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            1,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            [["data1to3_dpi0_voice0_sms0_val23to45", "1", "365.0"]],
        ),
        (
            "AAAAAA",
            "2020-03-09",
            30.0,
            60.0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            1,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            1,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            [
                ["data7to15_dpi1_voice0to1200_sms0_val23to45", "1", "60.0"],
                ["data0to1_dpi0_voice0to1200_sms1_val23to45", "1", "30.0"],
            ],
        ),
        (
            "AAAAAA",
            "2020-02-10",
            30.0,
            30.0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            1,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            [["data7to15_dpi1_voice0to1200_sms0_val23to45", "1", "30.0"]],
        ),
        (
            "BBBBBB",
            "2020-03-09",
            30.0,
            30.0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            1,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            [["data0_dpi0_voice0_sms1_val5to10", "1", "30.0"]],
        ),
    ]
    return data


@pytest.fixture
def product_agg_schema():
    schema = StructType(
        [
            StructField("msisdn", StringType()),
            StructField("weekstart", StringType()),
            StructField("fea_product_min_validity_period_days_01w", FloatType()),
            StructField("fea_product_max_validity_period_days_01w", FloatType()),
            StructField("fea_product_count_validity_1_to_4_days_01w", IntegerType()),
            StructField("fea_product_count_validity_5_to_10_days_01w", IntegerType()),
            StructField("fea_product_count_validity_11_to_22_days_01w", IntegerType()),
            StructField("fea_product_count_validity_23_to_45_days_01w", IntegerType()),
            StructField("fea_product_count_validity_46plus_days_01w", IntegerType()),
            StructField(
                "fea_product_count_data0_dpi0_voice0to1200_sms1_val23to45_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data15to32_dpi1_voice1200to400000_sms0_val46plus_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data32to64_dpi0_voice0_sms0_val23to45_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data7to15_dpi0_voice0to1200_sms1_val23to45_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data3to7_dpi0_voice0_sms0_val5to10_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data7to15_dpi1_voice0_sms0_val23to45_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data0_dpi0_voice0_sms0_val0to4_01w", IntegerType()
            ),
            StructField(
                "fea_product_count_data3to7_dpi0_voice0to1200_sms0_val23to45_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data7to15_dpi0_voice0_sms0_val5to10_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data3to7_dpi0_voice0to1200_sms1_val23to45_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data15to32_dpi1_voice0to1200_sms1_val23to45_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data7to15_dpi0_voice0_sms0_val23to45_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data0to1_dpi0_voice1200to400000_sms0_val23to45_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data3to7_dpi0_voice0to1200_sms1_val5to10_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data7to15_dpi0_voice400000to999999999_sms1_val23to45_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data0_dpi0_voice0to1200_sms0_val0to4_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data3to7_dpi1_voice0_sms0_val23to45_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data7to15_dpi1_voice0to1200_sms1_val23to45_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data3to7_dpi1_voice0to1200_sms1_val23to45_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data1to3_dpi0_voice0to1200_sms0_val23to45_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data0_dpi0_voice400000to999999999_sms0_val0to4_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data0_dpi0_voice0_sms1_val5to10_01w", IntegerType()
            ),
            StructField(
                "fea_product_count_data15to32_dpi1_voice0_sms0_val23to45_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data0to1_dpi0_voice0to1200_sms1_val23to45_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data0_dpi0_voice1200to400000_sms0_val5to10_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data1to3_dpi0_voice0_sms0_val23to45_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data0to1_dpi0_voice0_sms0_val0to4_01w", IntegerType()
            ),
            StructField(
                "fea_product_count_data15to32_dpi0_voice0_sms0_val23to45_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data0_dpi0_voice1200to400000_sms0_val11to22_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data32to64_dpi0_voice0to1200_sms1_val23to45_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data1to3_dpi0_voice0_sms0_val0to4_01w", IntegerType()
            ),
            StructField(
                "fea_product_count_data7to15_dpi1_voice0to1200_sms0_val23to45_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data15to32_dpi0_voice0to1200_sms1_val23to45_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data32to64_dpi1_voice0_sms0_val23to45_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data1to3_dpi0_voice0to1200_sms1_val23to45_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data7to15_dpi0_voice0_sms0_val0to4_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data1to3_dpi0_voice0to1200_sms1_val5to10_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data3to7_dpi0_voice0_sms0_val23to45_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data15to32_dpi1_voice0to1200_sms0_val23to45_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data0to1_dpi0_voice0_sms0_val5to10_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data15to32_dpi1_voice0_sms0_val46plus_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data3to7_dpi1_voice0to1200_sms0_val23to45_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data15to32_dpi1_voice0_sms0_val5to10_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data0_dpi0_voice1200to400000_sms0_val23to45_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data7to15_dpi1_voice0to1200_sms0_val46plus_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data0to1_dpi0_voice0to1200_sms0_val5to10_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data3to7_dpi0_voice0_sms0_val0to4_01w", IntegerType()
            ),
            StructField(
                "fea_product_count_data32to64_dpi0_voice400000to999999999_sms1_val23to45_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data0_dpi0_voice0_sms0_val5to10_01w", IntegerType()
            ),
            StructField(
                "fea_product_count_data0to1_dpi0_voice1200to400000_sms0_val5to10_01w",
                IntegerType(),
            ),
            StructField(
                "fea_product_count_data1to3_dpi0_voice0_sms0_val5to10_01w",
                IntegerType(),
            ),
            StructField("macroproduct_count_list", ArrayType(StringType())),
        ]
    )
    return schema


@mock.patch(
    "src.dmp.pipelines.dmp._04_features.product.fea_macroproduct.get_start_date",
    return_value=datetime.date(2018, 10, 10),
    autospec=True,
)
@mock.patch(
    "src.dmp.pipelines.dmp._04_features.product.fea_macroproduct.get_end_date",
    return_value=datetime.date(2020, 10, 10),
    autospec=True,
)
class TestFeaturesMacroproduct:
    def test_create_product_features(
        self,
        mock_get_start_date,
        mock_get_end_date,
        spark_session: SparkSession,
        product_agg_data: list,
        product_agg_schema: StructType,
    ):
        sc = spark_session.sparkContext

        df_agg = spark_session.createDataFrame(
            sc.parallelize(product_agg_data), schema=product_agg_schema
        )
        macroproduct_list = [
            "data0_dpi0_voice0to1200_sms1_val23to45",
            "data15to32_dpi1_voice1200to400000_sms0_val46plus",
            "data32to64_dpi0_voice0_sms0_val23to45",
            "data7to15_dpi0_voice0to1200_sms1_val23to45",
            "data3to7_dpi0_voice0_sms0_val5to10",
            "data7to15_dpi1_voice0_sms0_val23to45",
            "data0_dpi0_voice0_sms0_val0to4",
            "data3to7_dpi0_voice0to1200_sms0_val23to45",
            "data7to15_dpi0_voice0_sms0_val5to10",
            "data3to7_dpi0_voice0to1200_sms1_val23to45",
            "data15to32_dpi1_voice0to1200_sms1_val23to45",
            "data7to15_dpi0_voice0_sms0_val23to45",
            "data0to1_dpi0_voice1200to400000_sms0_val23to45",
            "data3to7_dpi0_voice0to1200_sms1_val5to10",
            "data7to15_dpi0_voice400000to999999999_sms1_val23to45",
            "data0_dpi0_voice0to1200_sms0_val0to4",
            "data3to7_dpi1_voice0_sms0_val23to45",
            "data7to15_dpi1_voice0to1200_sms1_val23to45",
            "data3to7_dpi1_voice0to1200_sms1_val23to45",
            "data1to3_dpi0_voice0to1200_sms0_val23to45",
            "data0_dpi0_voice400000to999999999_sms0_val0to4",
            "data0_dpi0_voice0_sms1_val5to10",
            "data15to32_dpi1_voice0_sms0_val23to45",
            "data0to1_dpi0_voice0to1200_sms1_val23to45",
            "data0_dpi0_voice1200to400000_sms0_val5to10",
            "data1to3_dpi0_voice0_sms0_val23to45",
            "data0to1_dpi0_voice0_sms0_val0to4",
            "data15to32_dpi0_voice0_sms0_val23to45",
            "data0_dpi0_voice1200to400000_sms0_val11to22",
            "data32to64_dpi0_voice0to1200_sms1_val23to45",
            "data1to3_dpi0_voice0_sms0_val0to4",
            "data7to15_dpi1_voice0to1200_sms0_val23to45",
            "data15to32_dpi0_voice0to1200_sms1_val23to45",
            "data32to64_dpi1_voice0_sms0_val23to45",
            "data1to3_dpi0_voice0to1200_sms1_val23to45",
            "data7to15_dpi0_voice0_sms0_val0to4",
            "data1to3_dpi0_voice0to1200_sms1_val5to10",
            "data3to7_dpi0_voice0_sms0_val23to45",
            "data15to32_dpi1_voice0to1200_sms0_val23to45",
            "data0to1_dpi0_voice0_sms0_val5to10",
            "data15to32_dpi1_voice0_sms0_val46plus",
            "data3to7_dpi1_voice0to1200_sms0_val23to45",
            "data15to32_dpi1_voice0_sms0_val5to10",
            "data0_dpi0_voice1200to400000_sms0_val23to45",
            "data7to15_dpi1_voice0to1200_sms0_val46plus",
            "data0to1_dpi0_voice0to1200_sms0_val5to10",
            "data3to7_dpi0_voice0_sms0_val0to4",
            "data32to64_dpi0_voice400000to999999999_sms1_val23to45",
            "data0_dpi0_voice0_sms0_val5to10",
            "data0to1_dpi0_voice1200to400000_sms0_val5to10",
            "data1to3_dpi0_voice0_sms0_val5to10",
        ]
        actual_result_df = create_product_features(
            df_agg, macroproduct_list, 28, "all", {"product": []}
        )

        expected_result_df = spark_session.read.parquet(
            "src/tests/unit_test/dmp/pipelines/dmp/_04_features/product/fea_macro_product.parquet"
        )

        assert_df_frame_equal(actual_result_df, expected_result_df, order_by=["msisdn"])
