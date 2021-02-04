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
import unittest

import numpy
import pandas
from pandas.testing import assert_frame_equal, assert_series_equal

# node functions to test
from src.dmp.pipelines.reseller.utils import (
    auc_average,
    master_remove_duplicates,
    master_sample,
    obtain_features_target,
    rmse,
    rrse,
    rse,
    unpack_if_single_element,
)


class TestResellerUtils(unittest.TestCase):
    def test_master_remove_duplicates(self):

        sort_by = ["outlet_id"]

        test_categorical_columns = ["cat1"]
        test_numerical_columns = ["num1", "min_dist_1"]

        test_model_params = {"target_variable": "target", "min_dist_max_value": 21000.0}

        test_input_df = pandas.DataFrame(
            {
                "outlet_id": [1, 1, 2, 3],
                "cat1": ["a", "a", "placeholder1", "placeholder2"],
                "num1": [1.0, 1.0, 50.0, 100.0],
                "min_dist_1": [numpy.nan, numpy.nan, 5000.0, 3000.0],
                "target": [300.0, 300.0, 200.0, 500.0],
            }
        )

        fn_master_remove_duplicates_df = master_remove_duplicates(
            test_input_df,
            test_numerical_columns,
            test_categorical_columns,
            test_model_params,
        )

        testcase_master_remove_duplicates_df = pandas.DataFrame(
            {
                "outlet_id": [1, 2, 3],
                "cat1": ["a", "placeholder1", "placeholder2"],
                "num1": [1.0, 50.0, 100.0],
                "min_dist_1": [21000.0, 5000.0, 3000.0],
                "target": [300.0, 200.0, 500.0],
            }
        )

        testcase_master_remove_duplicates_df.index = [0, 2, 3]

        assert_frame_equal(
            fn_master_remove_duplicates_df.sort_values(by=sort_by).sort_index(axis=1),
            testcase_master_remove_duplicates_df.sort_values(by=sort_by).sort_index(
                axis=1
            ),
        )

    def test_master_sample(self):

        sort_by = ["target"]

        test_model_params = {
            "target_variable": "target",
            "filter_specific_outlets_for_full_dataset": False,
            "include_specific_outlet_ids_in_test_set": False,
            "remove_target_zero": True,
            "remove_target_below_threshold": 5,
            "remove_target_below_quantile": 0.05,
            "remove_target_above_threshold": 10,
            "remove_target_above_quantile": 0.95,
            "pandas_sample_frac": {"frac": 1.0, "random_state": 15},
        }

        test_input_df = pandas.DataFrame(
            {"outlet_id": [1, 2, 3, 4, 5], "target": [0.0, 3.0, 8.0, 11.0, 5.0]}
        )

        fn_master_sample_df = master_sample(test_input_df, test_model_params)

        testcase_master_sample_df = pandas.DataFrame(
            {"outlet_id": [3, 5], "target": [8.0, 5.0]}
        )

        assert_frame_equal(
            fn_master_sample_df.sort_values(by=sort_by)
            .reset_index(drop=True)
            .sort_index(axis=1),
            testcase_master_sample_df.sort_values(by=sort_by)
            .reset_index(drop=True)
            .sort_index(axis=1),
        )

    def test_unpack_if_single_element(self):

        test_input_df = pandas.DataFrame({"target": [1.0]})

        fn_unpack_if_single_element_df = unpack_if_single_element([test_input_df])

        testcase_unpack_if_single_element_df = test_input_df

        assert_frame_equal(
            fn_unpack_if_single_element_df, testcase_unpack_if_single_element_df
        )

    def test_obtain_features_target(self):

        test_model_params = {
            "target_variable": "target",
            "model_type": "classification",
            "drop_columns": ["drop1"],
            "encode_features": True,
            "scale_features": True,
        }

        test_columns_to_encode = ["cat1"]
        test_columns_to_scale = ["num1"]

        test_input_df = pandas.DataFrame(
            {
                "target": [1.0],
                "target_binned": [0.0],
                "drop1": ["a"],
                "cat1": ["b"],
                "num1": [1.0],
                "cat1_encoded": [0.0],
                "num1_scaled": [0.5],
            }
        )

        fn_obtain_features_target_df = obtain_features_target(
            test_model_params,
            test_columns_to_encode,
            test_columns_to_scale,
            test_input_df,
        )

        testcase_obtain_features_target_df_features = pandas.DataFrame(
            {"cat1_encoded": [0.0], "num1_scaled": [0.5]}
        )

        testcase_obtain_features_target_series_target = pandas.Series([0.0])
        testcase_obtain_features_target_series_target.rename(
            "target_binned", inplace=True
        )

        assert_frame_equal(
            fn_obtain_features_target_df[0].sort_index(axis=1),
            testcase_obtain_features_target_df_features.sort_index(axis=1),
        )

        assert_series_equal(
            fn_obtain_features_target_df[1],
            testcase_obtain_features_target_series_target,
        )

    def test_auc_average(self):

        test_y_true = pandas.Series([1, 0, 2, 3, 1])

        test_y_pred_scores = pandas.DataFrame(
            {
                "class_0": [0.0, 1.0, 1.0, 0.0, 0.0],
                "class_1": [1.0, 0.0, 0.0, 0.0, 1.0],
                "class_2": [0.0, 0.0, 0.0, 0.0, 0.0],
                "class_3": [0.0, 0.0, 0.0, 1.0, 0.0],
            }
        )

        macro_auc = auc_average("macro")

        fn_macro_auc_float = macro_auc(test_y_true, test_y_pred_scores)

        testcase_macro_auc_float = 0.83

        assert round(fn_macro_auc_float, 2) == round(testcase_macro_auc_float, 2)

    def test_rmse(self):

        test_y_true = pandas.Series([1, 0, 2, 5, 1])

        test_y_pred = pandas.Series([1, 0, 2, 3, 1])

        fn_rmse_float = rmse(test_y_true, test_y_pred)

        testcase_rmse_float = numpy.sqrt(
            numpy.mean(numpy.square(numpy.subtract(test_y_true, test_y_pred)))
        )

        assert fn_rmse_float == testcase_rmse_float

    def test_rse(self):

        k = 1

        test_y_true = pandas.Series([1, 0, 2, 5, 1])

        test_y_pred = pandas.Series([1, 0, 2, 3, 1])

        fn_rse_float = rse(test_y_true, test_y_pred, k=k)

        deno = 3
        testcase_rse_float = numpy.sqrt(
            numpy.sum(numpy.square(test_y_true - test_y_pred)) / deno
        )

        assert fn_rse_float == testcase_rse_float

    def test_rrse(self):

        k = 1

        test_y_true = pandas.Series([1, 0, 2, 5, 1])

        test_y_pred = pandas.Series([1, 0, 2, 3, 1])

        fn_rrse_float = rrse(test_y_true, test_y_pred, k=k)

        deno = 3
        testcase_rrse_float = numpy.sqrt(
            numpy.sum(numpy.square(test_y_true - test_y_pred)) / deno
        ) / numpy.mean(test_y_true)

        assert fn_rrse_float == testcase_rrse_float
