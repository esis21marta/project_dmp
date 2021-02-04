import datetime
from unittest import mock

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.dmp.pipelines.dmp._04_features.text_messaging.fea_commercial_text_covid import (
    fea_comm_text_messaging_covid,
    fea_comm_text_messaging_covid_final,
)
from src.tests.pysaprk_df_equality import assert_df_frame_equal


class TestFeaCommercial:

    params = yaml.load(open("conf/dmp/training/parameters.yml"))

    config_feature = yaml.load(open("conf/dmp/training/parameters_feature.yml"))[
        "config_feature"
    ]

    required_output_features = yaml.load(
        open("conf/dmp/training/parameters_feature.yml")
    )["output_features"]

    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.text_messaging.fea_commercial_text_covid.get_start_date",
        return_value=datetime.date(2018, 10, 10),
        autospec=True,
    )
    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.text_messaging.fea_commercial_text_covid.get_end_date",
        return_value=datetime.date(2020, 10, 10),
        autospec=True,
    )
    def test_fea_comm_text_messaging(
        self, mock_get_start_date, mock_get_end_date, spark_session: SparkSession
    ):
        df_comm_txt_msg_weekly_pre_covid = spark_session.createDataFrame(
            data=[
                ["1", datetime.date(2020, 1, 20), "messaging", ["whatsapp"], 3],
                ["1", datetime.date(2020, 1, 27), "messaging", ["whatsapp"], 1],
                ["1", datetime.date(2020, 2, 24), "messaging", ["whatsapp"], 2],
                ["1", datetime.date(2020, 1, 20), "government", ["bapenda"], 3],
                ["1", datetime.date(2020, 1, 27), "government", ["bapenda"], 1],
                ["1", datetime.date(2020, 2, 24), "government", ["bapenda"], 2],
                ["1", datetime.date(2020, 1, 20), "restaurants", ["dd"], 3],
                ["1", datetime.date(2020, 1, 27), "restaurants", ["dd"], 1],
                ["1", datetime.date(2020, 2, 24), "restaurants", ["dd"], 2],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("weekstart", DateType(), False),
                    StructField("category", StringType(), True),
                    StructField("senders_07d", ArrayType(StringType(), True), True),
                    StructField("incoming_count_07d", LongType(), True),
                ]
            ),
        )

        df_comm_txt_msg_weekly_covid = spark_session.createDataFrame(
            data=[
                ["1", datetime.date(2020, 3, 23), "messaging", ["whatsapp"], 3],
                ["1", datetime.date(2020, 4, 27), "messaging", ["whatsapp", "line"], 1],
                ["1", datetime.date(2020, 5, 4), "messaging", ["whatsapp"], 2],
                ["1", datetime.date(2020, 3, 23), "government", ["bapenda"], 3],
                ["1", datetime.date(2020, 4, 27), "government", ["bapenda"], 1],
                ["1", datetime.date(2020, 5, 4), "government", ["bapenda"], 2],
                ["1", datetime.date(2020, 3, 23), "restaurants", ["dd"], 3],
                ["1", datetime.date(2020, 4, 27), "restaurants", ["dd"], 1],
                ["1", datetime.date(2020, 5, 4), "restaurants", ["dd"], 2],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("weekstart", DateType(), False),
                    StructField("category", StringType(), True),
                    StructField("senders_07d", ArrayType(StringType(), True), True),
                    StructField("incoming_count_07d", LongType(), True),
                ]
            ),
        )

        df_comm_txt_msg_weekly_post_covid = spark_session.createDataFrame(
            data=[
                ["1", datetime.date(2020, 6, 1), "messaging", ["whatsapp"], 3],
                ["1", datetime.date(2020, 7, 20), "messaging", ["whatsapp"], 1],
                ["1", datetime.date(2020, 8, 31), "messaging", ["whatsapp"], 2],
                ["1", datetime.date(2020, 6, 1), "government", ["bapenda"], 3],
                ["1", datetime.date(2020, 7, 20), "government", ["bapenda"], 1],
                ["1", datetime.date(2020, 8, 31), "government", ["bapenda"], 2],
                ["1", datetime.date(2020, 6, 1), "restaurants", ["dd"], 3],
                ["1", datetime.date(2020, 7, 20), "restaurants", ["dd"], 1],
                ["1", datetime.date(2020, 8, 31), "restaurants", ["dd"], 2],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("weekstart", DateType(), False),
                    StructField("category", StringType(), True),
                    StructField("senders_07d", ArrayType(StringType(), True), True),
                    StructField("incoming_count_07d", LongType(), True),
                ]
            ),
        )

        actual_fea_df = fea_comm_text_messaging_covid(
            df_comm_txt_msg_weekly_pre_covid,
            df_comm_txt_msg_weekly_covid,
            df_comm_txt_msg_weekly_post_covid,
            self.config_feature,
            "all",
            self.required_output_features,
        )

        expected_fea_df = spark_session.createDataFrame(
            data=[
                [
                    "1",
                    0.5,
                    0.5,
                    0.5,
                    0.5,
                    0.3333333333333333,
                    0.5,
                    0.5,
                    0.5,
                    0.5,
                    0.5,
                    0.5,
                    0.5,
                ],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), True),
                    StructField(
                        "fea_txt_msg_government_unique_senders_ratio_pre_covid_covid",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_government_incoming_ratio_pre_covid_covid",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_government_unique_senders_ratio_pre_covid_post_covid",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_government_incoming_ratio_pre_covid_post_covid",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_messaging_unique_senders_ratio_pre_covid_covid",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_restaurants_incoming_ratio_pre_covid_post_covid",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_messaging_unique_senders_ratio_pre_covid_post_covid",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_restaurants_incoming_ratio_pre_covid_covid",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_messaging_incoming_ratio_pre_covid_covid",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_restaurants_unique_senders_ratio_pre_covid_covid",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_messaging_incoming_ratio_pre_covid_post_covid",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_restaurants_unique_senders_ratio_pre_covid_post_covid",
                        DoubleType(),
                        True,
                    ),
                ]
            ),
        )

        assert_df_frame_equal(actual_fea_df, expected_fea_df, order_by=["msisdn"])

    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.text_messaging.fea_commercial_text_covid.get_start_date",
        return_value=datetime.date(2018, 10, 10),
        autospec=True,
    )
    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.text_messaging.fea_commercial_text_covid.get_end_date",
        return_value=datetime.date(2020, 10, 10),
        autospec=True,
    )
    def test_fea_comm_text_messaging_final(
        self, mock_get_start_date, mock_get_end_date, spark_session: SparkSession
    ):
        df_comm_txt_msg_weekly = spark_session.createDataFrame(
            data=[
                ["1", datetime.date(2019, 12, 16)],
                ["1", datetime.date(2020, 1, 27)],
                ["1", datetime.date(2020, 9, 21)],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), True),
                    StructField("weekstart", DateType(), True),
                ]
            ),
        )

        df_comm_txt_msg_weekly_covid = spark_session.createDataFrame(
            data=[
                [
                    "1",
                    0.5,
                    0.5,
                    0.5,
                    0.5,
                    0.3333333333333333,
                    0.5,
                    0.5,
                    0.5,
                    0.5,
                    0.5,
                    0.5,
                    0.5,
                ],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), True),
                    StructField(
                        "fea_txt_msg_government_unique_senders_ratio_pre_covid_covid",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_government_incoming_ratio_pre_covid_covid",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_government_unique_senders_ratio_pre_covid_post_covid",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_government_incoming_ratio_pre_covid_post_covid",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_messaging_unique_senders_ratio_pre_covid_covid",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_restaurants_incoming_ratio_pre_covid_post_covid",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_messaging_unique_senders_ratio_pre_covid_post_covid",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_restaurants_incoming_ratio_pre_covid_covid",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_messaging_incoming_ratio_pre_covid_covid",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_restaurants_unique_senders_ratio_pre_covid_covid",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_messaging_incoming_ratio_pre_covid_post_covid",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_restaurants_unique_senders_ratio_pre_covid_post_covid",
                        DoubleType(),
                        True,
                    ),
                ]
            ),
        )

        pre_covid_last_weekstart = datetime.date(2020, 2, 24)
        covid_last_weekstart = datetime.date(2020, 6, 1)
        post_covid_last_weekstart = datetime.date(2020, 8, 31)

        actual_fea_df = fea_comm_text_messaging_covid_final(
            df_comm_txt_msg_weekly,
            df_comm_txt_msg_weekly_covid,
            pre_covid_last_weekstart,
            covid_last_weekstart,
            post_covid_last_weekstart,
            self.config_feature,
            "all",
            self.required_output_features,
        )

        expected_fea_df = spark_session.createDataFrame(
            data=[
                [
                    "1",
                    datetime.date(2019, 12, 16),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                ],
                [
                    "1",
                    datetime.date(2020, 1, 27),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                ],
                [
                    "1",
                    datetime.date(2020, 9, 21),
                    0.5,
                    0.3333333333333333,
                    0.5,
                    0.5,
                    0.5,
                    0.5,
                    0.5,
                    0.5,
                    0.5,
                    0.5,
                    0.5,
                    0.5,
                ],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), True),
                    StructField("weekstart", DateType(), True),
                    StructField(
                        "fea_txt_msg_messaging_unique_senders_ratio_pre_covid_post_covid",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_messaging_unique_senders_ratio_pre_covid_covid",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_government_unique_senders_ratio_pre_covid_covid",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_government_unique_senders_ratio_pre_covid_post_covid",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_messaging_incoming_ratio_pre_covid_post_covid",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_restaurants_incoming_ratio_pre_covid_covid",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_restaurants_unique_senders_ratio_pre_covid_covid",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_government_incoming_ratio_pre_covid_covid",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_restaurants_incoming_ratio_pre_covid_post_covid",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_restaurants_unique_senders_ratio_pre_covid_post_covid",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_government_incoming_ratio_pre_covid_post_covid",
                        DoubleType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_messaging_incoming_ratio_pre_covid_covid",
                        DoubleType(),
                        True,
                    ),
                ]
            ),
        )

        assert_df_frame_equal(
            actual_fea_df, expected_fea_df, order_by=["msisdn", "weekstart"]
        )
