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
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import RobustScaler

# node functions to test
from src.dmp.pipelines.reseller.single_model.single import (
    bin_target_variable,
    create_categorical_encoder,
    create_numerical_scaler,
    encode_categorical_features,
    evaluate_predictions,
    extract_feature_importance,
    impute_nulls,
    predict_outlet_target_var,
    retrieve_similar_outlets_ground_truths,
    scale_numerical_features,
    train_model,
    train_test_dataset_split,
    typecast_columns,
)
from src.dmp.pipelines.reseller.utils import obtain_outlet_performance_percentile


def train_model_helper():

    test_training_features = pandas.DataFrame(
        {
            "num1": [3.0, 15.0, 8.0, 4.0, 10.0],
            "num2": [10.0, 11.0, 5.0, 30.0, 3.0],
            "num3": [52.0, 9.0, 8.0, 15.0, 7.0],
        }
    )

    test_training_targets = pandas.Series([0, 0, 1, 0, 2])

    test_model_params = {
        "model_type": "classification",
        "sklearn_params": {
            "n_estimators": 3,
            "max_features": 0.5,
            "random_state": 15,
            "n_jobs": 48,
            "verbose": 0,
            "oob_score": False,
        },
    }

    fn_output_model, _, max_leaves, trees_bootstrap_indices = train_model(
        test_training_features, test_training_targets, test_model_params
    )

    params = {
        "features": test_training_features,
        "targets": test_training_targets,
        "sklearn_params": test_model_params["sklearn_params"],
    }

    return fn_output_model, params, max_leaves, trees_bootstrap_indices


class TestResellerSingleModel(unittest.TestCase):
    def test_train_test_dataset_split(self):
        sort_by = "outlet_id"

        test_input_df = pandas.DataFrame(
            {
                "outlet_id": [
                    1100000031,
                    1100000098,
                    1100000660,
                    3175093253,
                    1085060328,
                    3058032585,
                    6403603862,
                    8624086032,
                    6340683252,
                    5032805832,
                    4626461636,
                    1346326267,
                    2364373456,
                    2465733756,
                    3464373472,
                ]
            }
        )

        # 1. testing include specific outlets in test set
        test_model_params = {
            "random_state": 15,
            "split_ratio": {"train": 0.8},
            "include_specific_outlet_ids_in_test_set": True,
        }

        (
            fn_output_train_df_specific,
            fn_output_test_df_specific,
        ) = train_test_dataset_split(test_input_df, test_model_params)

        # output dataframes
        testcase_train_df_specific = pandas.DataFrame(
            {
                "outlet_id": [
                    1100000031,
                    1100000098,
                    3175093253,
                    1085060328,
                    3058032585,
                    6403603862,
                    8624086032,
                    6340683252,
                    4626461636,
                    1346326267,
                    2364373456,
                    2465733756,
                ]
            }
        )
        testcase_test_df_specific = pandas.DataFrame(
            {"outlet_id": [1100000660, 5032805832, 3464373472]}
        )

        assert_frame_equal(
            fn_output_train_df_specific.sort_values(by=sort_by).sort_index(axis=1),
            testcase_train_df_specific.sort_values(by=sort_by).sort_index(axis=1),
        )

        assert_frame_equal(
            fn_output_test_df_specific.sort_values(by=sort_by).sort_index(axis=1),
            testcase_test_df_specific.sort_values(by=sort_by).sort_index(axis=1),
        )

        # 2. testing without specific outlets in test set
        test_model_params["include_specific_outlet_ids_in_test_set"] = False

        (
            fn_output_train_df_non_specific,
            fn_output_test_df_non_specific,
        ) = train_test_dataset_split(test_input_df, test_model_params)

        distinct_outlets = pandas.Series(test_input_df["outlet_id"].unique())
        testcase_train_outlets, testcase_test_outlets = train_test_split(
            distinct_outlets,
            random_state=test_model_params["random_state"],
            train_size=test_model_params["split_ratio"]["train"],
        )

        # output dataframes
        testcase_train_df_non_specific = test_input_df.loc[
            test_input_df["outlet_id"].isin(testcase_train_outlets), :
        ].reset_index(drop=True)
        testcase_test_df_non_specific = test_input_df.loc[
            test_input_df["outlet_id"].isin(testcase_test_outlets), :
        ].reset_index(drop=True)

        assert_frame_equal(
            fn_output_train_df_non_specific.sort_values(by=sort_by).sort_index(axis=1),
            testcase_train_df_non_specific.sort_values(by=sort_by).sort_index(axis=1),
        )

        assert_frame_equal(
            fn_output_test_df_non_specific.sort_values(by=sort_by).sort_index(axis=1),
            testcase_test_df_non_specific.sort_values(by=sort_by).sort_index(axis=1),
        )

    def test_bin_target_variable(self):
        sort_by = "outlet_id"

        test_input_df = pandas.DataFrame(
            {
                "outlet_id": [
                    1100000031,
                    1100000098,
                    1100000660,
                    3175093253,
                    1085060328,
                    1332639155,
                ],
                "target_variable": [1, 5, 3, 10, 9, 8],
            }
        )

        # testing equal size binning
        test_model_params = {
            "model_type": "classification",
            "classification_num_target_classes": 3,
            "target_variable": "target_variable",
        }

        _, fn_output_equal_size_df = bin_target_variable(
            test_model_params, test_input_df
        )

        testcase_equal_size_df = pandas.DataFrame(
            {
                "outlet_id": [
                    1100000031,
                    1100000098,
                    1100000660,
                    3175093253,
                    1085060328,
                    1332639155,
                ],
                "target_variable": [1, 5, 3, 10, 9, 8],
                "target_variable_binned": [0, 1, 0, 2, 2, 1],
            }
        ).astype(
            {
                "target_variable_binned": pandas.CategoricalDtype(
                    categories=None, ordered=True
                )
            }
        )

        assert_frame_equal(
            fn_output_equal_size_df.sort_values(by=sort_by).sort_index(axis=1),
            testcase_equal_size_df.sort_values(by=sort_by).sort_index(axis=1),
        )

    def test_impute_nulls(self):

        sort_by = ["num1"]

        test_categorical_columns = ["cat1"]
        test_numerical_columns = ["num1"]

        test_impute_params = {"categorical": "no_value", "numerical": 0.0}

        test_input_df = pandas.DataFrame(
            {
                "cat1": [None, numpy.nan, "placeholder1", "placeholder2"],
                "num1": [None, numpy.nan, 50.0, 100.0],
            }
        )

        fn_output_impute_nulls_df = impute_nulls(
            test_categorical_columns,
            test_numerical_columns,
            test_impute_params,
            test_input_df,
        )

        testcase_impute_nulls_df = pandas.DataFrame(
            {
                "cat1": ["no_value", "no_value", "placeholder1", "placeholder2"],
                "num1": [0.0, 0.0, 50.0, 100.0],
            }
        )

        assert_frame_equal(
            fn_output_impute_nulls_df.sort_values(by=sort_by).sort_index(axis=1),
            testcase_impute_nulls_df.sort_values(by=sort_by).sort_index(axis=1),
        )

    def test_typecast_columns(self):

        sort_by = ["num1"]

        test_categorical_columns = ["cat1"]
        test_numerical_columns = ["num1"]

        test_input_df = pandas.DataFrame(
            {"cat1": ["placeholder1", "placeholder2"], "num1": [50.0, 100.0]}
        )

        fn_output_typecast_columns_df = typecast_columns(
            test_categorical_columns, test_numerical_columns, test_input_df
        )

        testcase_typecast_columns_df = pandas.DataFrame(
            {"cat1": ["placeholder1", "placeholder2"], "num1": [50.0, 100.0]}
        ).astype({"cat1": str, "num1": float})

        assert_frame_equal(
            fn_output_typecast_columns_df.sort_values(by=sort_by).sort_index(axis=1),
            testcase_typecast_columns_df.sort_values(by=sort_by).sort_index(axis=1),
        )

    def test_create_categorical_encoder(self):

        test_columns_to_encode = ["cat1"]
        test_model_params = {"encode_features": True}

        test_training_set = pandas.DataFrame(
            {"cat1": ["placeholder1", "placeholder2", "placeholder3"]}
        )

        fn_output_create_categorical_encoder_dict = create_categorical_encoder(
            test_columns_to_encode, test_model_params, test_training_set
        )

        fn_output_categories_dict = fn_output_create_categorical_encoder_dict[
            "categories_dict"
        ]
        fn_output_ord_encoder_categories_ = fn_output_create_categorical_encoder_dict[
            "ord_encoder"
        ].categories_

        testcase_categories_dict = {
            "cat1": set(["placeholder1", "placeholder2", "placeholder3"])
        }
        testcase_ord_encoder_categories_ = [
            numpy.array(["placeholder1", "placeholder2", "placeholder3", "UNKNOWN"])
        ]

        self.assertEqual(fn_output_categories_dict, testcase_categories_dict)
        numpy.testing.assert_equal(
            fn_output_ord_encoder_categories_, testcase_ord_encoder_categories_
        )

    def test_encode_categorical_features(self):

        sort_by = ["cat1_encoded", "cat1"]

        test_columns_to_encode = ["cat1"]
        test_model_params = {"encode_features": True}

        test_training_set = pandas.DataFrame(
            {"cat1": ["placeholder1", "placeholder2", "placeholder3"]}
        )

        test_encoder_dict = create_categorical_encoder(
            test_columns_to_encode, test_model_params, test_training_set
        )

        test_test_set = pandas.DataFrame({"cat1": ["hello", "placeholder2", "world"]})

        fn_output_encode_categorical_features_df = encode_categorical_features(
            test_encoder_dict, test_columns_to_encode, test_model_params, test_test_set
        )

        testcase_encode_categorical_features_df = pandas.DataFrame(
            {
                "cat1": ["UNKNOWN", "placeholder2", "UNKNOWN"],
                "cat1_encoded": [3.0, 1.0, 3.0],
            }
        )

        assert_frame_equal(
            fn_output_encode_categorical_features_df.sort_values(by=sort_by).sort_index(
                axis=1
            ),
            testcase_encode_categorical_features_df.sort_values(by=sort_by).sort_index(
                axis=1
            ),
        )

    def test_create_numerical_scaler(self):

        test_columns_to_scale = ["num1"]
        test_model_params = {"scale_features": True}

        test_training_set = pandas.DataFrame({"num1": [5.0, 2.0, 1.0, 10.0, 15.0]})

        fn_output_create_numerical_scaler = create_numerical_scaler(
            test_columns_to_scale, test_model_params, test_training_set
        )
        fn_output_create_numerical_scaler_center = (
            fn_output_create_numerical_scaler.center_
        )
        fn_output_create_numerical_scaler_scale = (
            fn_output_create_numerical_scaler.scale_
        )

        testcase_create_numerical_scaler = RobustScaler().fit(
            test_training_set[test_columns_to_scale]
        )
        testcase_create_numerical_scaler_center = (
            testcase_create_numerical_scaler.center_
        )
        testcase_create_numerical_scaler_scale = testcase_create_numerical_scaler.scale_

        numpy.testing.assert_equal(
            fn_output_create_numerical_scaler_center,
            testcase_create_numerical_scaler_center,
        )
        numpy.testing.assert_equal(
            fn_output_create_numerical_scaler_scale,
            testcase_create_numerical_scaler_scale,
        )

    def test_scale_numerical_features(self):

        sort_by = ["num1"]

        test_columns_to_scale = ["num1"]
        test_model_params = {"scale_features": True}

        test_training_set = pandas.DataFrame({"num1": [5.0, 2.0, 1.0, 10.0, 15.0]})

        test_numerical_scaler = create_numerical_scaler(
            test_columns_to_scale, test_model_params, test_training_set
        )

        test_test_set = pandas.DataFrame({"num1": [3.0, 15.0, 8.0, 4.0, 10.0]})

        fn_output_scale_numerical_features_df = scale_numerical_features(
            test_columns_to_scale,
            test_numerical_scaler,
            test_model_params,
            test_test_set,
        )

        iqr = numpy.nanpercentile(test_training_set.num1, 75) - numpy.nanpercentile(
            test_training_set.num1, 25
        )
        median = numpy.nanmedian(test_training_set.num1)

        testcase_scale_numerical_features_df = pandas.DataFrame(
            {"num1": [3.0, 15.0, 8.0, 4.0, 10.0]}
        )
        testcase_scale_numerical_features_df["num1_scaled"] = (
            testcase_scale_numerical_features_df.num1 - median
        ) / iqr

        assert_frame_equal(
            fn_output_scale_numerical_features_df.sort_values(by=sort_by).sort_index(
                axis=1
            ),
            testcase_scale_numerical_features_df.sort_values(by=sort_by).sort_index(
                axis=1
            ),
        )

    def test_train_model(self):

        fn_output_model, test_params, _, _ = train_model_helper()
        fn_output_feature_importances = fn_output_model.feature_importances_

        testcase_model = RandomForestClassifier(
            criterion="gini", **test_params["sklearn_params"]
        ).fit(test_params["features"], test_params["targets"])
        testcase_feature_importances = testcase_model.feature_importances_

        numpy.testing.assert_equal(
            fn_output_feature_importances, testcase_feature_importances
        )

    def test_extract_feature_importance(self):

        test_model, test_params, _, _ = train_model_helper()
        test_train_set_columns = ["num1", "num2", "num3"]

        fn_output_feature_importances_df = extract_feature_importance(
            test_model, test_train_set_columns
        )

        test_feature_importances = test_model.feature_importances_

        sorted_importances_cols = [
            col
            for _, col in sorted(
                zip(test_feature_importances, test_train_set_columns), reverse=True
            )
        ]
        sorted_importances = sorted(test_feature_importances, reverse=True)
        testcase_feature_importances_df = pandas.DataFrame(
            {"feature": sorted_importances_cols, "importance": sorted_importances}
        )

        assert_frame_equal(
            fn_output_feature_importances_df, testcase_feature_importances_df
        )

    def test_predict_outlet_target_var(self):

        sort_by = ["outlet_id"]

        test_model, test_params, _, _ = train_model_helper()

        test_model_params = {
            "model_type": "classification",
            "target_variable": "target",
        }

        test_features = pandas.DataFrame(
            {
                "num1": [5.0, 10.0, 2.0,],
                "num2": [8.0, 11.0, 5.0,],
                "num3": [52.0, 14.0, 4.0,],
            }
        )

        test_prepared = pandas.DataFrame(
            {
                "num1": [5.0, 10.0, 2.0,],
                "num2": [8.0, 11.0, 5.0,],
                "num3": [52.0, 14.0, 4.0,],
                "outlet_id": [1.0, 2.0, 3.0],
                "target": [10, 5, 7],
            }
        )

        (
            fn_output_prepared_dataset,
            fn_output_leaf_node_indices,
        ) = predict_outlet_target_var(
            test_model, test_model_params, test_features, test_prepared
        )

        testcase_predictions = test_model.predict(test_features)
        testcase_probas_columns = [
            f'{test_model_params["target_variable"]}_binned_prediction_probas_class_{class_num}'
            for class_num in range(3)
        ]
        testcase_predictions_probas_df = pandas.DataFrame(
            test_model.predict_proba(test_features),
            columns=testcase_probas_columns,
            index=test_prepared.index,
        )
        test_prepared[
            f'{test_model_params["target_variable"]}_binned_prediction'
        ] = testcase_predictions
        testcase_prepared_dataset = pandas.concat(
            [test_prepared, testcase_predictions_probas_df], axis=1
        )
        testcase_leaf_node_indices = pandas.DataFrame(test_model.apply(test_features))

        assert_frame_equal(
            fn_output_prepared_dataset.sort_values(by=sort_by).sort_index(axis=1),
            testcase_prepared_dataset.sort_values(by=sort_by).sort_index(axis=1),
        )

        assert_frame_equal(fn_output_leaf_node_indices, testcase_leaf_node_indices)

    def test_evaluate_predictions(self):

        test_features_columns = ["num1", "num2", "num3"]

        test_model, test_params, _, _ = train_model_helper()

        test_model_params = {
            "model_type": "classification",
            "target_variable": "target",
            "classification_num_target_classes": 3,
        }

        test_features = pandas.DataFrame(
            {
                "num1": [3.0, 15.0, 8.0, 4.0, 10.0],
                "num2": [10.0, 11.0, 5.0, 30.0, 3.0],
                "num3": [52.0, 9.0, 8.0, 15.0, 7.0],
            }
        )

        test_prepared = pandas.DataFrame(
            {
                "num1": [3.0, 15.0, 8.0, 4.0, 10.0],
                "num2": [10.0, 11.0, 5.0, 30.0, 3.0],
                "num3": [52.0, 9.0, 8.0, 15.0, 7.0],
                "outlet_id": [1.0, 2.0, 3.0, 4.0, 5.0],
                "target_binned": [0, 0, 1, 0, 2],
            }
        )

        test_prediction_df, _ = predict_outlet_target_var(
            test_model, test_model_params, test_features, test_prepared
        )

        fn_output_evaluation_metrics_df = evaluate_predictions(
            test_model_params, test_features_columns, test_prediction_df
        )

        testcase_evaluation_metrics_df = pandas.DataFrame(
            {"dataset_num": [1], "auc_weighted": [1.0]}
        )

        assert_frame_equal(
            fn_output_evaluation_metrics_df, testcase_evaluation_metrics_df
        )

    def test_build_training_indices_matrix(self):

        # assert grouped by leaf node pandas chained function
        test_pandas_chain_df = pandas.DataFrame({"num1": [3.0, 3.0, 8.0, 4.0, 4.0]})

        fn_output_test_pandas_chain_series = (
            test_pandas_chain_df.reset_index()
            .groupby("num1")
            .index.apply(list)
            .sort_index(ascending=True)
        )

        testcase_pandas_chain_series = pandas.Series(
            data=[[0, 1], [3, 4], [2]],
            index=[3.0, 4.0, 8.0],
            dtype="object",
            name="index",
        )
        testcase_pandas_chain_series.index.names = ["num1"]

        # assert pandas group by value and collect index
        assert_series_equal(
            fn_output_test_pandas_chain_series, testcase_pandas_chain_series
        )

    def test_retrieve_similar_outlets_ground_truths(self):

        test_predictions_leaf_indices = pandas.DataFrame(
            {0: [1, 2, 3], 1: [4, 5, 6], 2: [7, 8, 9]}
        )

        test_training_indices_matrix = pandas.DataFrame(
            {
                0: [*[[num] for num in range(1, 11)]],
                1: [*[[num] for num in range(11, 21)]],
                2: [*[[num] for num in range(21, 31)]],
            }
        )

        test_training_set = pandas.DataFrame(
            {"target": [num for num in range(100, 3200, 100)]}
        )

        test_prepared_dataset_of_predictions = pandas.DataFrame(
            {"outlet_id": [10, 20, 30], "target": [793, 193, 315]}
        )

        test_predictions = pandas.DataFrame({"target_binned_prediction": [1, 0, 2]})

        test_model_params = {
            "target_variable": "target",
            "model_type": "classification",
        }

        fn_output_retrieve_similar_outlets_ground_truths_df = retrieve_similar_outlets_ground_truths(
            test_predictions_leaf_indices,
            test_training_indices_matrix,
            test_training_set,
            test_prepared_dataset_of_predictions,
            test_predictions,
            test_model_params,
        )

        testcase_retrieve_similar_outlets_ground_truths_df = pandas.DataFrame(
            {
                "outlet_id": [10, 20, 30],
                "ground_truth": [793, 193, 315],
                "prediction": [1, 0, 2],
                "training_ground_truths_list": [[300, 1600], [400, 1700], [500, 1800]],
            }
        )

        assert_frame_equal(
            fn_output_retrieve_similar_outlets_ground_truths_df,
            testcase_retrieve_similar_outlets_ground_truths_df,
        )

    def test_obtain_outlet_performance_percentile(self):

        test_performance_percentiles = pandas.DataFrame(
            {
                "outlet_id": [1, 2, 3],
                "training_ground_truths_list": [
                    [5.0, 10.0, 11.0, 15.0],
                    [2.0, 4.0, 5.0],
                    [3.0, 5.0, 7.0],
                ],
                "ground_truth": [5.0, 3.0, 10.0],
            }
        )

        fn_output_obtain_outlet_performance_percentile_df = obtain_outlet_performance_percentile(
            test_performance_percentiles
        ).drop(
            columns=[
                "potential_cashflow_90th_percentile",
                "potential_cashflow_95th_percentile",
                "potential_cashflow_100th_percentile",
            ]
        )

        testcase_test_obtain_outlet_performance_percentile_df = pandas.DataFrame(
            {
                "outlet_id": [1, 2, 3],
                "training_ground_truths_list": [
                    [5.0, 10.0, 11.0, 15.0, 5.0],
                    [2.0, 4.0, 5.0, 3.0],
                    [3.0, 5.0, 7.0, 10.0],
                ],
                "ground_truth": [5.0, 3.0, 10.0],
                "training_ground_truths_list_length": [4, 3, 3],
                "performance_percentile": [0.0, 25.0, 75.0],
            }
        )

        assert_frame_equal(
            fn_output_obtain_outlet_performance_percentile_df.sort_index(axis=1),
            testcase_test_obtain_outlet_performance_percentile_df.sort_index(axis=1),
        )
