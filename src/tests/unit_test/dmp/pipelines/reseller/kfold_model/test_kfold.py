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

# node functions to test
from src.dmp.pipelines.reseller.kfold_model.kfold import (
    kfold_bin_target,
    kfold_encode_categorical,
    kfold_evaluate,
    kfold_impute_nulls,
    kfold_master_split,
    kfold_obtain_features_target,
    kfold_predict,
    kfold_retrieve_similar_outlets_ground_truths,
    kfold_scale_numerical,
    kfold_train_models,
    kfold_typecast_columns,
)


def train_kfold_model_helper():

    test_input_dict = {
        0: {
            "train_set": {
                "features": pandas.DataFrame(
                    {
                        "num1": [3.0, 15.0, 8.0, 4.0, 10.0],
                        "num2": [10.0, 11.0, 5.0, 30.0, 3.0],
                        "num3": [52.0, 9.0, 8.0, 15.0, 7.0],
                    }
                ),
                "target": pandas.Series([0, 0, 1, 0, 2]),
            }
        }
    }

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

    (
        fn_output_kfold_models_dict,
        fn_output_kfold_feature_columns,
        fn_output_kfold_training_matrix_info_dict,
    ) = kfold_train_models(test_input_dict, test_model_params)

    params = {
        "features": test_input_dict[0]["train_set"]["features"],
        "targets": test_input_dict[0]["train_set"]["target"],
        "sklearn_params": test_model_params["sklearn_params"],
    }

    return (
        fn_output_kfold_models_dict,
        params,
        fn_output_kfold_training_matrix_info_dict,
    )


class TestResellerSingleModel(unittest.TestCase):
    def test_kfold_master_split(self):
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
                ]
            }
        )

        test_model_params = {
            "kfold": {"n_splits": 2, "shuffle": True, "random_state": 15}
        }

        fn_output_kfold_split_dict = kfold_master_split(
            test_input_df, test_model_params
        )
        fn_output_kfold_0_train_df = fn_output_kfold_split_dict[0]["train_set"]
        fn_output_kfold_1_test_df = fn_output_kfold_split_dict[1]["test_set"]

        # output dataframes
        testcase_train_df_specific = pandas.DataFrame(
            {
                "outlet_id": [
                    1100000031,
                    3175093253,
                    3058032585,
                    8624086032,
                    6340683252,
                    4626461636,
                    2364373456,
                ]
            }
        )

        assert_frame_equal(
            fn_output_kfold_0_train_df.sort_values(by=sort_by).sort_index(axis=1),
            testcase_train_df_specific.sort_values(by=sort_by).sort_index(axis=1),
        )

        assert_frame_equal(
            fn_output_kfold_1_test_df.sort_values(by=sort_by).sort_index(axis=1),
            testcase_train_df_specific.sort_values(by=sort_by).sort_index(axis=1),
        )

    def test_kfold_bin_target(self):
        sort_by = "outlet_id"

        test_input_dict = {
            0: {
                "train_set": pandas.DataFrame(
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
                ),
                "test_set": pandas.DataFrame(
                    {
                        "outlet_id": [
                            1100000031,
                            1100000098,
                            1100000660,
                            3175093253,
                            1085060328,
                            1332639155,
                        ],
                        "target_variable": [2, 6, 2, 9, 10, 7],
                    }
                ),
            }
        }

        # testing equal size binning
        test_model_params = {
            "model_type": "classification",
            "classification_num_target_classes": 3,
            "target_variable": "target_variable",
        }

        fn_output_kfold_bin_target_dict = kfold_bin_target(
            test_input_dict, test_model_params
        )

        testcase_kfold_bin_target_test_df = pandas.DataFrame(
            {
                "outlet_id": [
                    1100000031,
                    1100000098,
                    1100000660,
                    3175093253,
                    1085060328,
                    1332639155,
                ],
                "target_variable": [2, 6, 2, 9, 10, 7],
                "target_variable_binned": [0, 1, 0, 2, 2, 1],
            }
        )

        assert_frame_equal(
            fn_output_kfold_bin_target_dict[0]["test_set"]
            .sort_values(by=sort_by)
            .sort_index(axis=1),
            testcase_kfold_bin_target_test_df.sort_values(by=sort_by).sort_index(
                axis=1
            ),
        )

    def test_kfold_impute_nulls(self):

        sort_by = ["outlet_id"]

        test_categorical_columns = ["cat1"]
        test_numerical_columns = ["num1"]

        test_impute_params = {"categorical": "no_value", "numerical": 0.0}

        test_input_dict = {
            0: {
                "train_set": pandas.DataFrame(
                    {
                        "outlet_id": [1100000031, 1100000098, 1100000660, 3175093253],
                        "cat1": [None, numpy.nan, "placeholder1", "placeholder2"],
                        "num1": [None, numpy.nan, 50.0, 100.0],
                    }
                ),
                "test_set": pandas.DataFrame(
                    {
                        "outlet_id": [1100000031, 1100000098, 1100000660, 3175093253],
                        "cat1": [None, numpy.nan, None, numpy.nan],
                        "num1": [None, numpy.nan, None, numpy.nan],
                    }
                ),
            }
        }

        fn_output_kfold_impute_nulls_dict = kfold_impute_nulls(
            test_input_dict,
            test_categorical_columns,
            test_numerical_columns,
            test_impute_params,
        )

        testcase_impute_nulls_train_df = pandas.DataFrame(
            {
                "outlet_id": [1100000031, 1100000098, 1100000660, 3175093253],
                "cat1": ["no_value", "no_value", "placeholder1", "placeholder2"],
                "num1": [0.0, 0.0, 50.0, 100.0],
            }
        )

        testcase_impute_nulls_test_df = pandas.DataFrame(
            {
                "outlet_id": [1100000031, 1100000098, 1100000660, 3175093253],
                "cat1": ["no_value", "no_value", "no_value", "no_value"],
                "num1": [0.0, 0.0, 0.0, 0.0],
            }
        )

        assert_frame_equal(
            fn_output_kfold_impute_nulls_dict[0]["train_set"]
            .sort_values(by=sort_by)
            .sort_index(axis=1),
            testcase_impute_nulls_train_df.sort_values(by=sort_by).sort_index(axis=1),
        )

        assert_frame_equal(
            fn_output_kfold_impute_nulls_dict[0]["test_set"]
            .sort_values(by=sort_by)
            .sort_index(axis=1),
            testcase_impute_nulls_test_df.sort_values(by=sort_by).sort_index(axis=1),
        )

    def test_kfold_typecast_columns(self):

        sort_by = ["outlet_id"]

        test_categorical_columns = ["cat1"]
        test_numerical_columns = ["num1"]

        test_input_dict = {
            0: {
                "train_set": pandas.DataFrame(
                    {
                        "outlet_id": [1100000031, 1100000098],
                        "cat1": ["placeholder1", "placeholder2"],
                        "num1": [50.0, 100.0],
                    }
                ),
                "test_set": pandas.DataFrame(
                    {
                        "outlet_id": [1100000660, 3175093253],
                        "cat1": ["placeholder1", "placeholder2"],
                        "num1": [50.0, 70.0],
                    }
                ),
            }
        }

        fn_output_kfold_typecast_columns_dict = kfold_typecast_columns(
            test_input_dict, test_categorical_columns, test_numerical_columns
        )

        testcase_kfold_typecast_columns_train_df = pandas.DataFrame(
            {
                "outlet_id": [1100000031, 1100000098],
                "cat1": ["placeholder1", "placeholder2"],
                "num1": [50.0, 100.0],
            }
        ).astype({"cat1": str, "num1": float})

        assert_frame_equal(
            fn_output_kfold_typecast_columns_dict[0]["train_set"]
            .sort_values(by=sort_by)
            .sort_index(axis=1),
            testcase_kfold_typecast_columns_train_df.sort_values(by=sort_by).sort_index(
                axis=1
            ),
        )

    def test_kfold_encode_categorical(self):

        sort_by = ["cat1_encoded", "cat1"]

        test_columns_to_encode = ["cat1"]
        test_model_params = {"encode_features": True}

        test_input_dict = {
            0: {
                "train_set": pandas.DataFrame(
                    {"cat1": ["placeholder1", "placeholder2", "placeholder3"]}
                ),
                "test_set": pandas.DataFrame(
                    {"cat1": ["placeholder1", "no_value", "placeholder4"]}
                ),
            }
        }

        fn_output_kfold_encode_categorical_dict = kfold_encode_categorical(
            test_input_dict, test_columns_to_encode, test_model_params
        )

        testcase_kfold_encode_categorical_test_df = pandas.DataFrame(
            {
                "cat1": ["placeholder1", "UNKNOWN", "UNKNOWN"],
                "cat1_encoded": [0.0, 3.0, 3.0],
            }
        )

        assert_frame_equal(
            fn_output_kfold_encode_categorical_dict[0]["test_set"]
            .sort_values(by=sort_by)
            .sort_index(axis=1),
            testcase_kfold_encode_categorical_test_df.sort_values(
                by=sort_by
            ).sort_index(axis=1),
        )

    def test_kfold_scale_numerical(self):

        sort_by = ["num1"]

        test_columns_to_scale = ["num1"]
        test_model_params = {"scale_features": True}

        test_input_dict = {
            0: {
                "train_set": pandas.DataFrame({"num1": [5.0, 2.0, 1.0, 10.0, 15.0]}),
                "test_set": pandas.DataFrame({"num1": [3.0, 15.0, 8.0, 4.0, 10.0]}),
            }
        }

        fn_output_kfold_scale_numerical_dict = kfold_scale_numerical(
            test_input_dict, test_columns_to_scale, test_model_params,
        )

        iqr = numpy.nanpercentile(
            test_input_dict[0]["train_set"].num1, 75
        ) - numpy.nanpercentile(test_input_dict[0]["train_set"].num1, 25)
        median = numpy.nanmedian(test_input_dict[0]["train_set"].num1)

        testcase_scale_numerical_features_df = pandas.DataFrame(
            {"num1": [3.0, 15.0, 8.0, 4.0, 10.0]}
        )
        testcase_scale_numerical_features_df["num1_scaled"] = (
            testcase_scale_numerical_features_df.num1 - median
        ) / iqr

        assert_frame_equal(
            fn_output_kfold_scale_numerical_dict[0]["test_set"]
            .sort_values(by=sort_by)
            .sort_index(axis=1),
            testcase_scale_numerical_features_df.sort_values(by=sort_by).sort_index(
                axis=1
            ),
        )

    def test_kfold_obtain_features_target(self):

        test_model_params = {
            "target_variable": "target",
            "model_type": "classification",
            "drop_columns": ["drop1"],
            "encode_features": True,
            "scale_features": True,
        }

        test_columns_to_encode = ["cat1"]
        test_columns_to_scale = ["num1"]

        test_input_dict = {
            0: {
                "train_set": pandas.DataFrame(
                    {
                        "target": [1.0],
                        "target_binned": [0.0],
                        "drop1": ["a"],
                        "cat1": ["b"],
                        "num1": [1.0],
                        "cat1_encoded": [0.0],
                        "num1_scaled": [0.5],
                    }
                ),
                "test_set": pandas.DataFrame(
                    {
                        "target": [2.0],
                        "target_binned": [5.0],
                        "drop1": ["a"],
                        "cat1": ["e"],
                        "num1": [1.0],
                        "cat1_encoded": [0.0],
                        "num1_scaled": [0.5],
                    }
                ),
            }
        }

        fn_obtain_kfold_obtain_features_target_dict = kfold_obtain_features_target(
            test_input_dict,
            test_columns_to_encode,
            test_columns_to_scale,
            test_model_params,
        )

        testcase_kfold_obtain_features_target_train_features = pandas.DataFrame(
            {"cat1_encoded": [0.0], "num1_scaled": [0.5]}
        )

        testcase_kfold_obtain_features_target_train_target = pandas.Series([0.0])
        testcase_kfold_obtain_features_target_train_target.rename(
            "target_binned", inplace=True
        )

        assert_frame_equal(
            fn_obtain_kfold_obtain_features_target_dict[0]["train_set"][
                "features"
            ].sort_index(axis=1),
            testcase_kfold_obtain_features_target_train_features.sort_index(axis=1),
        )

        assert_series_equal(
            fn_obtain_kfold_obtain_features_target_dict[0]["train_set"]["target"],
            testcase_kfold_obtain_features_target_train_target,
        )

    def test_kfold_train_models(self):

        fn_output_kfold_models_dict, test_params, _ = train_kfold_model_helper()
        fn_output_feature_importances = fn_output_kfold_models_dict[
            0
        ].feature_importances_

        testcase_model = RandomForestClassifier(
            criterion="gini", **test_params["sklearn_params"]
        ).fit(test_params["features"], test_params["targets"])
        testcase_feature_importances = testcase_model.feature_importances_

        numpy.testing.assert_equal(
            fn_output_feature_importances, testcase_feature_importances
        )

    def test_kfold_predict(self):

        sort_by = ["outlet_id"]

        test_kfold_models, _, _ = train_kfold_model_helper()

        test_model_params = {
            "model_type": "classification",
            "target_variable": "target",
        }

        test_kfold_train_test_sets_features_target_dict = {
            0: {
                "train_set": {
                    "features": pandas.DataFrame(
                        {
                            "num1": [5.0, 10.0, 2.0,],
                            "num2": [8.0, 11.0, 5.0,],
                            "num3": [52.0, 14.0, 4.0,],
                        }
                    )
                },
                "test_set": {
                    "features": pandas.DataFrame(
                        {
                            "num1": [5.0, 10.0, 2.0,],
                            "num2": [8.0, 11.0, 5.0,],
                            "num3": [52.0, 14.0, 4.0,],
                        }
                    )
                },
            }
        }
        test_kfold_train_test_sets_dict = {
            0: {
                "train_set": pandas.DataFrame(
                    {
                        "num1": [5.0, 10.0, 2.0,],
                        "num2": [8.0, 11.0, 5.0,],
                        "num3": [52.0, 14.0, 4.0,],
                        "outlet_id": [1.0, 2.0, 3.0],
                        "target": [10, 5, 7],
                    }
                ),
                "test_set": pandas.DataFrame(
                    {
                        "num1": [5.0, 10.0, 2.0,],
                        "num2": [8.0, 11.0, 5.0,],
                        "num3": [52.0, 14.0, 4.0,],
                        "outlet_id": [1.0, 2.0, 3.0],
                        "target": [10, 5, 7],
                    }
                ),
            }
        }

        (
            fn_output_kfold_train_test_sets_prediction_dict,
            fn_output_kfold_leaf_indices_sets_dict,
        ) = kfold_predict(
            test_kfold_train_test_sets_features_target_dict,
            test_kfold_train_test_sets_dict,
            test_kfold_models,
            test_model_params,
        )

        test_features = test_kfold_train_test_sets_features_target_dict[0]["train_set"][
            "features"
        ]
        test_prepared = test_kfold_train_test_sets_dict[0]["train_set"]

        testcase_predictions = test_kfold_models[0].predict(test_features)
        testcase_probas_columns = [
            f'{test_model_params["target_variable"]}_binned_prediction_probas_class_{class_num}'
            for class_num in range(3)
        ]
        testcase_predictions_probas_df = pandas.DataFrame(
            test_kfold_models[0].predict_proba(test_features),
            columns=testcase_probas_columns,
            index=test_prepared.index,
        )
        test_prepared[
            f'{test_model_params["target_variable"]}_binned_prediction'
        ] = testcase_predictions
        testcase_prepared_dataset = pandas.concat(
            [test_prepared, testcase_predictions_probas_df], axis=1
        )
        testcase_leaf_node_indices = pandas.DataFrame(
            test_kfold_models[0].apply(test_features)
        )

        assert_frame_equal(
            fn_output_kfold_train_test_sets_prediction_dict[0]["train_set"]
            .sort_values(by=sort_by)
            .sort_index(axis=1),
            testcase_prepared_dataset.sort_values(by=sort_by).sort_index(axis=1),
        )

        assert_frame_equal(
            fn_output_kfold_leaf_indices_sets_dict[0]["train_set"],
            testcase_leaf_node_indices,
        )

    def test_evaluate_predictions(self):

        test_features_columns = ["num1", "num2", "num3"]

        test_kfold_models, _, _ = train_kfold_model_helper()

        test_model_params = {
            "model_type": "classification",
            "target_variable": "target",
            "classification_num_target_classes": 3,
        }

        test_kfold_train_test_sets_features_target_dict = {
            0: {
                "train_set": {
                    "features": pandas.DataFrame(
                        {
                            "num1": [3.0, 15.0, 8.0, 4.0, 10.0],
                            "num2": [10.0, 11.0, 5.0, 30.0, 3.0],
                            "num3": [52.0, 9.0, 8.0, 15.0, 7.0],
                        }
                    )
                },
                "test_set": {
                    "features": pandas.DataFrame(
                        {
                            "num1": [3.0, 15.0, 8.0, 4.0, 10.0],
                            "num2": [10.0, 11.0, 5.0, 30.0, 3.0],
                            "num3": [52.0, 9.0, 8.0, 15.0, 7.0],
                        }
                    )
                },
            }
        }
        test_kfold_train_test_sets_dict = {
            0: {
                "train_set": pandas.DataFrame(
                    {
                        "num1": [3.0, 15.0, 8.0, 4.0, 10.0],
                        "num2": [10.0, 11.0, 5.0, 30.0, 3.0],
                        "num3": [52.0, 9.0, 8.0, 15.0, 7.0],
                        "outlet_id": [1.0, 2.0, 3.0, 4.0, 5.0],
                        "target_binned": [0, 0, 1, 0, 2],
                    }
                ),
                "test_set": pandas.DataFrame(
                    {
                        "num1": [3.0, 15.0, 8.0, 4.0, 10.0],
                        "num2": [10.0, 11.0, 5.0, 30.0, 3.0],
                        "num3": [52.0, 9.0, 8.0, 15.0, 7.0],
                        "outlet_id": [1.0, 2.0, 3.0, 4.0, 5.0],
                        "target_binned": [0, 0, 1, 0, 2],
                    }
                ),
            }
        }

        fn_output_kfold_train_test_sets_prediction_dict, _ = kfold_predict(
            test_kfold_train_test_sets_features_target_dict,
            test_kfold_train_test_sets_dict,
            test_kfold_models,
            test_model_params,
        )

        fn_output_evaluation_metrics_df = kfold_evaluate(
            fn_output_kfold_train_test_sets_prediction_dict,
            test_model_params,
            test_features_columns,
        )

        testcase_evaluation_metrics_df = pandas.DataFrame(
            {
                "aggregate_function": [
                    "mean_test_set",
                    "std_test_set",
                    "median_test_set",
                    "min_test_set",
                    "max_test_set",
                ],
                "auc_weighted": [1.0, numpy.nan, 1.0, 1.0, 1.0],
            }
        )

        assert_frame_equal(
            fn_output_evaluation_metrics_df, testcase_evaluation_metrics_df
        )

    def test_kfold_retrieve_similar_outlets_ground_truths(self):

        test_predictions_leaf_indices_dict = {
            0: {
                "test_set": pandas.DataFrame({0: [1, 2, 3], 1: [4, 5, 6], 2: [7, 8, 9]})
            }
        }

        test_training_indices_matrices_dict = {
            0: pandas.DataFrame(
                {
                    0: [*[[num] for num in range(1, 11)]],
                    1: [*[[num] for num in range(11, 21)]],
                    2: [*[[num] for num in range(21, 31)]],
                }
            )
        }

        test_kfold_train_test_sets_dict = {
            0: {
                "train_set": pandas.DataFrame(
                    {"target": [num for num in range(100, 3200, 100)]}
                ),
                "test_set": pandas.DataFrame(
                    {"outlet_id": [10, 20, 30], "target": [793, 193, 315]}
                ),
            }
        }

        test_kfold_train_test_sets_prediction_dict = {
            0: {"test_set": pandas.DataFrame({"target_binned_prediction": [1, 0, 2]})}
        }

        test_model_params = {
            "target_variable": "target",
            "model_type": "classification",
        }

        fn_output_kfold_retrieve_similar_outlets_ground_truths_df = kfold_retrieve_similar_outlets_ground_truths(
            test_predictions_leaf_indices_dict,
            test_training_indices_matrices_dict,
            test_kfold_train_test_sets_dict,
            test_kfold_train_test_sets_prediction_dict,
            test_model_params,
        )

        testcase_kfold_retrieve_similar_outlets_ground_truths_df = pandas.DataFrame(
            {
                "outlet_id": [10, 20, 30],
                "ground_truth": [793, 193, 315],
                "prediction": [1, 0, 2],
                "training_ground_truths_list": [[300, 1600], [400, 1700], [500, 1800]],
            }
        )

        assert_frame_equal(
            fn_output_kfold_retrieve_similar_outlets_ground_truths_df,
            testcase_kfold_retrieve_similar_outlets_ground_truths_df,
        )
