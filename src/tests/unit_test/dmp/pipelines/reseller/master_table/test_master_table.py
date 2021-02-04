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

# third-party libraries
import pandas
from pyspark.sql import Row, SparkSession

# node functions to test
from src.dmp.pipelines.reseller.master_table.master_table import (
    agg_dynamic_categorical_features_mode,
    agg_dynamic_features_last,
    agg_dynamic_numerical_features_max,
    agg_dynamic_numerical_features_sum_mean,
    build_target_variable,
    create_lagged_features,
    filter_consecutive_zero_sales,
    filter_dynamic_master_month,
    filter_outlet_created_at_date,
    join_agg_dynamic_features,
    join_ds_outlets_master_table,
    prepare_non_tsf_outlet_ids,
    prepare_outlets_static_features,
)


class TestOutletMaster:
    def test_filter_outlet_created_at_date(self, spark_session: SparkSession):
        sc = spark_session.sparkContext

        df_filter_created_at_date = sc.parallelize(
            [
                ("1", "2019-09-03", "2020-01-02"),
                ("2", "2019-10-01", "2020-02-02"),
                ("3", "2019-09-09", "2020-03-02"),
                ("4", "2019-10-09", "2020-04-02"),
                ("5", "2019-09-07", "2020-05-02"),
            ]
        )

        df_filter_created_at_date = spark_session.createDataFrame(
            df_filter_created_at_date.map(
                lambda x: Row(outlet_id=x[0], fea_outlet_created_at=x[1], trx_date=x[2])
            )
        )

        master_table_params = {
            "to_date_format": "yyyy-MM-dd",
            "filter_month": "2019-10-01",
            "agg_date": "trx_date",
            "save_filtered_outlets": True,
            "agg_primary_columns": ["outlet_id", "month"],
        }

        df_dynamic_non_tsf = prepare_non_tsf_outlet_ids(
            df_filter_created_at_date, master_table_params
        )

        out, out_filtered_outlets = filter_outlet_created_at_date(
            df_filter_created_at_date, master_table_params, df_dynamic_non_tsf
        )

        out_list = [
            [i[0], i[1], i[2], i[3].strftime("%Y-%m-%d")]
            for i in out.select(
                "outlet_id", "fea_outlet_created_at", "trx_date", "month"
            ).collect()
        ]

        assert sorted(out_list) == [
            ["1", "2019-09-03", "2020-01-02", "2020-01-01"],
            ["3", "2019-09-09", "2020-03-02", "2020-03-01"],
            ["5", "2019-09-07", "2020-05-02", "2020-05-01"],
        ]

    def test_build_target_variable(self, spark_session: SparkSession):
        sc = spark_session.sparkContext

        df_build_target_variable = sc.parallelize(
            [("1", 1, 5), ("2", 2, None), ("5", None, None)]
        )

        df_build_target_variable = spark_session.createDataFrame(
            df_build_target_variable.map(
                lambda x: Row(outlet_id=x[0], mkios_cashflow=x[1], pv_cashflow=x[2])
            )
        )

        master_table_params = {
            "target_variable_components": ["mkios_cashflow", "pv_cashflow"],
            "target_variable": "total_cashflow",
        }

        out = build_target_variable(df_build_target_variable, master_table_params)

        out_list = [
            [i[0], i[1], i[2], i[3]]
            for i in out.select(
                "outlet_id", "mkios_cashflow", "pv_cashflow", "total_cashflow"
            ).collect()
        ]

        assert sorted(out_list) == [
            ["1", 1, 5, 6],
            ["2", 2, None, 2],
            ["5", None, None, 0],
        ]

    def test_filter_consecutive_zero_sales(self, spark_session: SparkSession):
        sc = spark_session.sparkContext

        df_filter_consecutive_zero_sales = sc.parallelize(
            [
                ("1", "2019-09-01", 0, "2019-09-01"),
                ("1", "2019-09-02", 0, "2019-09-01"),
                ("1", "2019-09-03", 0, "2019-09-01"),
                ("1", "2019-09-04", 1, "2019-09-01"),
                ("1", "2019-09-05", 2, "2019-09-01"),
                ("2", "2019-09-03", 0, "2019-09-01"),
                ("2", "2019-09-04", 0, "2019-09-01"),
                ("2", "2019-09-05", 2, "2019-09-01"),
                ("3", "2019-09-03", 0, "2019-09-01"),
                ("3", "2019-09-04", 1, "2019-09-01"),
                ("3", "2019-09-05", 2, "2019-09-01"),
            ]
        )

        df_filter_consecutive_zero_sales = spark_session.createDataFrame(
            df_filter_consecutive_zero_sales.map(
                lambda x: Row(
                    outlet_id=x[0], trx_date=x[1], total_cashflow=x[2], month=x[3]
                )
            )
        )

        master_table_params = {
            "consecutive_days_zero_sale": 2,
            "agg_primary_columns": ["outlet_id", "month"],
            "agg_date": "trx_date",
            "target_variable": "total_cashflow",
            "filter_month": "2019-09-01",
            "save_filtered_outlets": True,
            "to_date_format": "yyyy-MM-dd",
        }

        df_dynamic_non_tsf = prepare_non_tsf_outlet_ids(
            df_filter_consecutive_zero_sales, master_table_params
        )

        out, out_filtered_outlets = filter_consecutive_zero_sales(
            df_filter_consecutive_zero_sales, master_table_params, df_dynamic_non_tsf
        )

        out_list = [
            [i[0], i[1], i[2], i[3]]
            for i in out.select(
                "outlet_id", "trx_date", "total_cashflow", "month"
            ).collect()
        ]

        assert sorted(out_list) == [
            ["3", "2019-09-03", 0, "2019-09-01"],
            ["3", "2019-09-04", 1, "2019-09-01"],
            ["3", "2019-09-05", 2, "2019-09-01"],
        ]

    def test_filter_dynamic_master_month(self, spark_session: SparkSession):
        sc = spark_session.sparkContext

        df_filter_dynamic_master_month = sc.parallelize(
            [
                ("1", "2019-08-01"),
                ("1", "2019-09-01"),
                ("1", "2019-10-01"),
                ("1", "2019-11-01"),
                ("1", "2019-12-01"),
                ("2", "2019-08-01"),
                ("2", "2019-10-01"),
                ("2", "2019-12-01"),
            ]
        )

        df_filter_dynamic_master_month = spark_session.createDataFrame(
            df_filter_dynamic_master_month.map(
                lambda x: Row(outlet_id=x[0], month=x[1])
            )
        )

        master_table_params = {
            "agg_primary_columns": ["outlet_id", "month"],
            "filter_month": "2019-10-01",
        }

        out = filter_dynamic_master_month(
            df_filter_dynamic_master_month, master_table_params
        )

        out_list = [[i[0], i[1]] for i in out.select("outlet_id", "month").collect()]

        assert sorted(out_list) == [["1", "2019-10-01"], ["2", "2019-10-01"]]

    def test_agg_dynamic_numerical_features_sum_mean(self, spark_session: SparkSession):
        sc = spark_session.sparkContext

        df_agg_dynamic_numerical_features_sum_mean = sc.parallelize(
            [
                ("1", "2019-07-01", 3),
                ("1", "2019-07-01", 4),
                ("1", "2019-08-01", 5),
                ("1", "2019-08-01", 3),
                ("1", "2019-09-01", 6),
                ("1", "2019-09-01", 0),
                ("1", "2019-09-01", 3),
                ("1", "2019-10-01", 5),
                ("1", "2019-10-01", 0),
                ("1", "2019-10-01", 5),
                ("1", "2019-10-01", 0),
                ("1", "2019-11-01", 11),
                ("1", "2019-12-01", 6),
                ("1", "2019-12-01", 6),
            ]
        )

        df_agg_dynamic_numerical_features_sum_mean = spark_session.createDataFrame(
            df_agg_dynamic_numerical_features_sum_mean.map(
                lambda x: Row(outlet_id=x[0], month=x[1], total_cashflow=x[2])
            )
        )

        master_table_params = {
            "agg_primary_columns": ["outlet_id", "month"],
            "filter_month": pandas.to_datetime("2019-10-01").date(),
            "lagged_numerical_features_months": 3,
            "agg_numerical_columns": ["total_cashflow"],
            "target_variable": "total_cashflow",
        }

        out = agg_dynamic_numerical_features_sum_mean(
            df_agg_dynamic_numerical_features_sum_mean, master_table_params
        )

        out_list = [
            [i[0], i[1], i[2], i[3]]
            for i in out.select(
                "outlet_id", "month", "total_cashflow_mean", "total_cashflow_sum",
            ).collect()
        ]

        assert sorted(out_list) == [
            ["1", "2019-07-01", 3.5, 7],
            ["1", "2019-08-01", 4.0, 8],
            ["1", "2019-09-01", 4.5, 9],
            ["1", "2019-10-01", 5.0, 10],
        ]

    def test_create_lagged_features(self, spark_session: SparkSession):
        sc = spark_session.sparkContext

        df_create_lagged_features = sc.parallelize(
            [
                ("1", "2019-07-01", 3.5, 7),
                ("1", "2019-08-01", 4.0, 8),
                ("1", "2019-09-01", 4.5, 9),
                ("1", "2019-10-01", 5.0, 10),
            ]
        )

        df_create_lagged_features = spark_session.createDataFrame(
            df_create_lagged_features.map(
                lambda x: Row(
                    outlet_id=x[0],
                    month=x[1],
                    total_cashflow_mean=x[2],
                    total_cashflow_sum=x[3],
                )
            )
        )

        master_table_params = {
            "agg_primary_columns": ["outlet_id", "month"],
            "filter_month": pandas.to_datetime("2019-10-01"),
            "lagged_numerical_features_months": 3,
        }

        out = create_lagged_features(df_create_lagged_features, master_table_params)

        out_list = [
            [i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9]]
            for i in out.select(
                "outlet_id",
                "month",
                "total_cashflow_mean",
                "total_cashflow_sum",
                "total_cashflow_mean_past_1m",
                "total_cashflow_sum_past_1m",
                "total_cashflow_mean_past_2m",
                "total_cashflow_sum_past_2m",
                "total_cashflow_mean_past_3m",
                "total_cashflow_sum_past_3m",
            ).collect()
        ]

        assert sorted(out_list) == [
            ["1", "2019-10-01", 5.0, 10, 4.5, 9, 4.0, 8, 3.5, 7],
        ]

    def test_agg_dynamic_categorical_features_mode(self, spark_session: SparkSession):
        sc = spark_session.sparkContext

        df_agg_dynamic_categorical_features_mode = sc.parallelize(
            [
                ("1", "2019-08-01", "telco"),
                ("1", "2019-08-01", "telco"),
                ("1", "2019-08-01", "telco"),
                ("1", "2019-08-01", "non-telco"),
                ("1", "2019-09-01", "telco"),
                ("1", "2019-09-01", "telco"),
                ("1", "2019-09-01", "non-telco"),
                ("1", "2019-09-01", "non-telco"),
            ]
        )

        df_agg_dynamic_categorical_features_mode = spark_session.createDataFrame(
            df_agg_dynamic_categorical_features_mode.map(
                lambda x: Row(outlet_id=x[0], month=x[1], fea_outlet_string_type=x[2])
            )
        )

        master_table_params = {
            "agg_mode_columns": ["fea_outlet_string_type"],
            "agg_primary_columns": ["outlet_id", "month"],
        }

        out = agg_dynamic_categorical_features_mode(
            df_agg_dynamic_categorical_features_mode, master_table_params
        )

        out_list = [
            [i[0], i[1], i[2], i[3]]
            for i in out.select(
                "outlet_id",
                "month",
                "fea_outlet_string_type",
                "fea_outlet_string_type_count",
            ).collect()
        ]

        assert sorted(out_list) == [
            ["1", "2019-08-01", "telco", 3],
            ["1", "2019-09-01", "non-telco", 2],
        ]

    def test_agg_dynamic_features_last(self, spark_session: SparkSession):
        sc = spark_session.sparkContext

        df_agg_dynamic_features_last = sc.parallelize(
            [
                ("1", "2019-08-01", "2019-08-01", 1),
                ("1", "2019-08-01", "2019-08-03", 3),
                ("1", "2019-08-01", "2019-08-05", 5),
                ("1", "2019-08-01", "2019-08-20", 20),
                ("1", "2019-09-01", "2019-09-01", 1),
                ("1", "2019-09-01", "2019-09-08", 8),
                ("1", "2019-09-01", "2019-09-15", 15),
                ("1", "2019-09-01", "2019-09-30", 30),
            ]
        )

        df_agg_dynamic_features_last = spark_session.createDataFrame(
            df_agg_dynamic_features_last.map(
                lambda x: Row(
                    outlet_id=x[0],
                    month=x[1],
                    trx_date=x[2],
                    fea_outlet_integer_los=x[3],
                )
            )
        )

        master_table_params = {
            "agg_date": "trx_date",
            "agg_primary_columns": ["outlet_id", "month"],
            "agg_last_columns": ["fea_outlet_integer_los"],
        }

        out = agg_dynamic_features_last(
            df_agg_dynamic_features_last, master_table_params
        )

        out_list = [
            [i[0], i[1], i[2]]
            for i in out.select(
                "outlet_id", "month", "fea_outlet_integer_los"
            ).collect()
        ]

        assert sorted(out_list) == [["1", "2019-08-01", 20], ["1", "2019-09-01", 30]]

    def test_agg_dynamic_numerical_features_max(self, spark_session: SparkSession):
        sc = spark_session.sparkContext

        df_agg_dynamic_numerical_features_max = sc.parallelize(
            [
                ("1", "2019-08-01", 5),
                ("1", "2019-08-01", 3),
                ("1", "2019-08-01", 5),
                ("1", "2019-08-01", 19),
                ("1", "2019-09-01", 7),
                ("1", "2019-09-01", 3),
                ("1", "2019-09-01", 100),
                ("1", "2019-09-01", 8),
            ]
        )

        df_agg_dynamic_numerical_features_max = spark_session.createDataFrame(
            df_agg_dynamic_numerical_features_max.map(
                lambda x: Row(
                    outlet_id=x[0], month=x[1], fea_rs_msisdn_distinct_count=x[2]
                )
            )
        )

        master_table_params = {
            "agg_primary_columns": ["outlet_id", "month"],
            "agg_max_columns": ["fea_rs_msisdn_distinct_count"],
        }

        out = agg_dynamic_numerical_features_max(
            df_agg_dynamic_numerical_features_max, master_table_params
        )

        out_list = [
            [i[0], i[1], i[2]]
            for i in out.select(
                "outlet_id", "month", "fea_rs_msisdn_distinct_count_max"
            ).collect()
        ]

        assert sorted(out_list) == [["1", "2019-08-01", 19], ["1", "2019-09-01", 100]]

    def test_join_agg_dynamic_features(self, spark_session: SparkSession):
        sc = spark_session.sparkContext

        # first dataframe (max)
        df_join_agg_dynamic_features_max = sc.parallelize(
            [("1", "2019-08-01", 5), ("1", "2019-09-01", 7)]
        )

        df_join_agg_dynamic_features_max = spark_session.createDataFrame(
            df_join_agg_dynamic_features_max.map(
                lambda x: Row(
                    outlet_id=x[0], month=x[1], fea_rs_msisdn_distinct_count_max=x[2]
                )
            )
        )

        # second dataframe (mode)
        df_join_agg_dynamic_features_mode = sc.parallelize(
            [("1", "2019-08-01", "telco", 3), ("1", "2019-09-01", "non-telco", 2)]
        )

        df_join_agg_dynamic_features_mode = spark_session.createDataFrame(
            df_join_agg_dynamic_features_mode.map(
                lambda x: Row(
                    outlet_id=x[0],
                    month=x[1],
                    fea_outlet_string_type=x[2],
                    fea_outlet_string_type_count=x[3],
                )
            )
        )

        master_table_params = {
            "agg_primary_columns": ["outlet_id", "month"],
            "save_filtered_outlets": True,
            "filter_month": "2019-08-01",
        }

        out = join_agg_dynamic_features(
            master_table_params,
            df_join_agg_dynamic_features_max,
            df_join_agg_dynamic_features_mode,
        )

        out_list = [
            [i[0], i[1], i[2], i[3], i[4]]
            for i in out.select(
                "outlet_id",
                "month",
                "fea_rs_msisdn_distinct_count_max",
                "fea_outlet_string_type",
                "fea_outlet_string_type_count",
            ).collect()
        ]

        assert sorted(out_list) == [
            ["1", "2019-08-01", 5, "telco", 3],
            ["1", "2019-09-01", 7, "non-telco", 2],
        ]

    def test_prepare_outlets_static_features(self, spark_session: SparkSession):
        sc = spark_session.sparkContext

        df_prepare_outlets_static_features = sc.parallelize(
            [
                ("1", 1, 2, 3, 4, 5, "2020-01-01",),
                ("2", 11, 22, 33, 44, 55, "2020-01-01",),
                ("3", 111, 222, 333, 444, 555, "2020-01-01",),
            ]
        )

        df_prepare_outlets_static_features = spark_session.createDataFrame(
            df_prepare_outlets_static_features.map(
                lambda x: Row(
                    outlet_id=x[0],
                    geo1=x[1],
                    geo2=x[2],
                    geo3=x[3],
                    geo4=x[4],
                    geo5=x[5],
                    month=x[6],
                )
            )
        )

        master_table_params = {
            "agg_primary_columns": ["outlet_id", "month"],
            "static_columns": ["geo1", "geo3", "geo5"],
            "filter_month": "2020-01-01",
        }

        out = prepare_outlets_static_features(
            df_prepare_outlets_static_features, master_table_params
        )

        out_list = [
            [i[0], i[1], i[2], i[3]]
            for i in out.select("outlet_id", "geo1", "geo3", "geo5").collect()
        ]

        assert sorted(out_list) == [
            ["1", 1, 3, 5],
            ["2", 11, 33, 55],
            ["3", 111, 333, 555],
        ]

    def test_join_ds_outlets_master_table(self, spark_session: SparkSession):
        sc = spark_session.sparkContext

        sort_by = ["outlet_id", "month"]

        # first dataframe (revenue - dynamic)
        df_join_ds_outlets_master_table_rev = sc.parallelize(
            [
                ("1", "2019-08-01", "gold", -11.2085669, 94.7717124),
                ("1", "2019-09-01", "platinum", -11.2085669, 94.7717124),
                ("2", "2019-08-01", "bronze", 6.2744496, 141.0194444),
                ("2", "2019-09-01", "bronze", 6.2744496, 141.0194444),
            ]
        )

        df_join_ds_outlets_master_table_rev = spark_session.createDataFrame(
            df_join_ds_outlets_master_table_rev.map(
                lambda x: Row(
                    outlet_id=x[0],
                    month=x[1],
                    fea_outlet_string_classification=x[2],
                    fea_outlet_string_location_latitude=x[3],
                    fea_outlet_string_location_longitude=x[4],
                )
            )
        )

        # second dataframe (geospatial - static)
        df_join_ds_outlets_master_table_geo = sc.parallelize([("1", 5), ("2", 7)])

        df_join_ds_outlets_master_table_geo = spark_session.createDataFrame(
            df_join_ds_outlets_master_table_geo.map(
                lambda x: Row(outlet_id=x[0], geospatial_feature=x[1])
            )
        )

        master_table_params = {
            "agg_primary_columns": ["outlet_id", "month"],
            "save_filtered_outlets": True,
            "filter_month": "2019-08-01",
            "to_date_format": "yyyy-MM-dd",
            "agg_date": "trx_date",
        }

        df_dynamic_non_tsf = df_join_ds_outlets_master_table_rev.select(
            "outlet_id"
        ).distinct()

        out, out_filtered_outlets = join_ds_outlets_master_table(
            master_table_params,
            df_join_ds_outlets_master_table_rev,
            df_join_ds_outlets_master_table_geo,
            df_dynamic_non_tsf,
        )

        out_df = out[
            [
                "outlet_id",
                "month",
                "fea_outlet_string_classification",
                "geospatial_feature",
                "fea_outlet_string_location_latitude",
                "fea_outlet_string_location_longitude",
            ]
        ]

        testcase_df = pandas.DataFrame(
            {
                "outlet_id": ["1", "1", "2", "2"],
                "month": ["2019-08-01", "2019-09-01", "2019-08-01", "2019-09-01"],
                "fea_outlet_string_classification": [
                    "gold",
                    "platinum",
                    "bronze",
                    "bronze",
                ],
                "geospatial_feature": [5, 5, 7, 7],
                "fea_outlet_string_location_latitude": [
                    -11.2085669,
                    -11.2085669,
                    6.2744496,
                    6.2744496,
                ],
                "fea_outlet_string_location_longitude": [
                    94.7717124,
                    94.7717124,
                    141.0194444,
                    141.0194444,
                ],
            }
        )

        pandas.testing.assert_frame_equal(
            out_df.sort_values(by=sort_by).sort_index(axis=1),
            testcase_df.sort_values(by=sort_by).sort_index(axis=1),
        )
