import datetime
from unittest import mock

import yaml
from pyspark import Row
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, LongType, StringType, StructField, StructType

from src.dmp.pipelines.dmp._04_features.handset_internal.fea_handset_internal_all import (
    calculate_count_distincts,
    retrieve_mode,
    retrieve_week_windows,
)
from src.tests.pysaprk_df_equality import assert_df_frame_equal


@mock.patch(
    "src.dmp.pipelines.dmp._04_features.handset_internal.fea_handset_internal_all.get_start_date",
    return_value=datetime.date(2018, 10, 10),
    autospec=True,
)
@mock.patch(
    "src.dmp.pipelines.dmp._04_features.handset_internal.fea_handset_internal_all.get_end_date",
    return_value=datetime.date(2020, 10, 10),
    autospec=True,
)
class TestFeatureHandsetInternalAll:
    config_feature = yaml.load(open("conf/dmp/training/parameters_feature.yml"))[
        "config_feature"
    ]

    def test_retrieve_mode(
        self, mock_get_start_date, mock_get_end_date, spark_session: SparkSession
    ):
        sc = spark_session.sparkContext

        data = sc.parallelize(
            [
                # Case A) Sanity test - Get the mode from a single column (list of strings)
                ("111", "2019-10-01", ["a", "b", "a", "b", "a", "e"]),
                # Case B) Sanity test - Get the mode from a single column (list of integers)
                ("222", "2019-10-08", [1, 2, 1, 1, 5]),
                # Case C) Edge Case - Get mode from a single column with a lot of nulls
                (
                    "333",
                    "2019-10-08",
                    ["a", None, None, "", "b", "b", None, None, "", ""],
                ),
                # Case D) Edge Case - Get mode from a single column with only nulls
                ("444", "2019-10-08", [None, None, None]),
            ]
        )

        data = spark_session.createDataFrame(
            data.map(lambda x: Row(msisdn=x[0], weekstart=x[1], col=x[2]))
        )

        out = retrieve_mode(data, "all", [])

        assert sorted(out.columns) == [
            "fea_handset_internal_mode_col",
            "msisdn",
            "weekstart",
        ]

        out_list = [
            [i[0], i[1], str(i[2])]
            for i in out.select(
                "msisdn", "weekstart", "fea_handset_internal_mode_col"
            ).collect()
        ]
        assert sorted(out_list) == [
            # Case A) Select 'a' because of the most occurrence
            ["111", "2019-10-01", "a"],
            # Case B) Select 1 because of the most occurence
            ["222", "2019-10-08", "1"],
            # Case C) Select ''
            ["333", "2019-10-08", ""],
            # Case D) Row is removed since they are all None
            ["444", "2019-10-08", "None"],
        ]

    def test_name_conventions(
        self, mock_get_start_date, mock_get_end_date, spark_session: SparkSession
    ):
        sc = spark_session.sparkContext

        data = sc.parallelize(
            [
                (
                    "111",  # msisdn
                    "2019-10-01",  # weekstart
                    "manufacture-1",  # manufacture
                    "design_type-1",  # design_type
                    "device_type-1",  # device_type
                    "os-1" "network-1",  # os  # network
                    "volte-1",  # volte
                    "multisim-1",  # multisim
                    "card_type-1",  # card_type
                    "tac-1",  # tac
                )
            ]
        )

        data = spark_session.createDataFrame(
            data.map(
                lambda x: Row(
                    msisdn=x[0],
                    weekstart=x[1],
                    manufacture=x[2],
                    design_type=x[3],
                    device_type=x[4],
                    os=x[5],
                    network=x[6],
                    volte=x[7],
                    multisim=x[8],
                    card_type=x[8],
                    tac=x[9],
                )
            )
        )

        windows = retrieve_week_windows(
            data, feature_mode="all", required_output_features=[]
        )
        features = retrieve_mode(
            windows, feature_mode="all", required_output_features=[]
        )

        assert sorted(features.columns) == sorted(
            [
                "msisdn",
                "weekstart",
                "fea_handset_internal_mode_manufacture_00w_to_01w",
                "fea_handset_internal_mode_manufacture_02w_to_01m",
                "fea_handset_internal_mode_design_type_00w_to_01w",
                "fea_handset_internal_mode_design_type_02w_to_01m",
                "fea_handset_internal_mode_device_type_00w_to_01w",
                "fea_handset_internal_mode_device_type_02w_to_01m",
                "fea_handset_internal_mode_os_00w_to_01w",
                "fea_handset_internal_mode_os_02w_to_01m",
                "fea_handset_internal_mode_network_00w_to_01w",
                "fea_handset_internal_mode_network_02w_to_01m",
                "fea_handset_internal_mode_volte_00w_to_01w",
                "fea_handset_internal_mode_volte_02w_to_01m",
                "fea_handset_internal_mode_multisim_00w_to_01w",
                "fea_handset_internal_mode_multisim_02w_to_01m",
                "fea_handset_internal_mode_card_type_00w_to_01w",
                "fea_handset_internal_mode_card_type_02w_to_01m",
                "fea_handset_internal_mode_tac_00w_to_01w",
                "fea_handset_internal_mode_tac_02w_to_01m",
            ]
        )

    def test_calculate_count_distincts(
        self, mock_get_start_date, mock_get_end_date, spark_session: SparkSession
    ):

        data = spark_session.createDataFrame(
            data=[
                # Case A) Sanity case with null row (due to scaffold)
                ["1", "tac", "man1", datetime.date(2019, 1, 1)],
                ["1", "tac", "man1", datetime.date(2019, 1, 8)],
                ["1", None, None, datetime.date(2019, 1, 15)],
                ["1", "tac1", "man", datetime.date(2019, 1, 22)],
                # Case B) Sanity case
                ["2", "tac1", "man1", datetime.date(2019, 1, 1)],
                # Case C) Sanity case
                ["3", "tac", "man", datetime.date(2019, 1, 1)],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("tac", StringType(), True),
                    StructField("manufacture", StringType(), True),
                    StructField("weekstart", DateType(), False),
                ]
            ),
        )

        output = calculate_count_distincts(data, "all", [])
        actual_tacs = output["tac_count_df"]
        actual_mans = output["man_count_df"]

        expected_tacs = spark_session.createDataFrame(
            data=[
                [2, "tac", datetime.date(2019, 1, 1)],
                [1, "tac", datetime.date(2019, 1, 8)],
                [1, "tac1", datetime.date(2019, 1, 22)],
                [1, "tac1", datetime.date(2019, 1, 1)],
                [1, None, datetime.date(2019, 1, 15)],
            ],
            schema=StructType(
                [
                    StructField("fea_handset_int_same_tac_count", LongType(), True),
                    StructField("tac", StringType(), True),
                    StructField("weekstart", DateType(), True),
                ]
            ),
        )

        expected_mans = spark_session.createDataFrame(
            data=[
                ["man1", 2, datetime.date(2019, 1, 1)],
                ["man1", 1, datetime.date(2019, 1, 8)],
                ["man", 1, datetime.date(2019, 1, 22)],
                ["man", 1, datetime.date(2019, 1, 1)],
                [None, 1, datetime.date(2019, 1, 15)],
            ],
            schema=StructType(
                [
                    StructField("manufacture", StringType(), True),
                    StructField(
                        "fea_handset_int_same_manufacture_count", LongType(), True
                    ),
                    StructField("weekstart", DateType(), True),
                ]
            ),
        )

        assert_df_frame_equal(actual_tacs, expected_tacs)
        assert_df_frame_equal(actual_mans, expected_mans)
