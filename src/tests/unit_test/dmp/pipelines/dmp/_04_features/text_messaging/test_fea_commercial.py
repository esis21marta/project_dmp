import datetime
from unittest import mock

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.dmp.pipelines.dmp._04_features.text_messaging.fea_commercial_text import (
    fea_comm_text_messaging,
)
from src.tests.pysaprk_df_equality import assert_df_frame_equal


class TestFeaCommercial:
    config_feature = yaml.load(open("conf/dmp/training/parameters_feature.yml"))[
        "config_feature"
    ]

    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.text_messaging.fea_commercial_text.get_start_date",
        return_value=datetime.date(2018, 10, 10),
        autospec=True,
    )
    @mock.patch(
        "src.dmp.pipelines.dmp._04_features.text_messaging.fea_commercial_text.get_end_date",
        return_value=datetime.date(2020, 10, 10),
        autospec=True,
    )
    def test_fea_comm_text_messaging(
        self, mock_get_start_date, mock_get_end_date, spark_session: SparkSession
    ):
        comm_agg_df = spark_session.createDataFrame(
            data=[
                ["6281122", datetime.date(2020, 2, 3), "messaging", ["whatsapp"], 3, 0],
                ["6281122", datetime.date(2020, 1, 6), "government", ["bapenda"], 1, 1],
                ["6281122", datetime.date(2020, 2, 3), "restaurants", ["dd"], 1, 0],
                [
                    "6281123",
                    datetime.date(2020, 2, 3),
                    "ecommerce shopping",
                    ["lazada", "shopee"],
                    5,
                    0,
                ],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("weekstart", DateType(), False),
                    StructField("category", StringType(), True),
                    StructField("senders_07d", ArrayType(StringType(), True), True),
                    StructField("incoming_count_07d", LongType(), True),
                    StructField(
                        "count_txt_msg_incoming_government_tax", LongType(), True
                    ),
                ]
            ),
        )

        df_kredivo_sms_recipients = spark_session.createDataFrame(
            data=[["6281122", datetime.date(2020, 2, 3),],],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("weekstart", DateType(), False),
                ]
            ),
        )

        actual_fea_df = fea_comm_text_messaging(
            comm_agg_df,
            df_kredivo_sms_recipients,
            self.config_feature,
            feature_mode="all",
            required_output_features=[],
        )

        expected_fea_df = spark_session.createDataFrame(
            data=[
                [
                    "6281122",
                    datetime.date(2020, 1, 6),
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
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
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
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    1,
                ],
                [
                    "6281122",
                    datetime.date(2020, 2, 3),
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
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    0,
                    0,
                    0,
                    0,
                    1,
                    1,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    3,
                    3,
                    3,
                    3,
                    3,
                    3,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                ],
                [
                    "6281123",
                    datetime.date(2020, 2, 3),
                    5,
                    5,
                    5,
                    5,
                    5,
                    5,
                    2,
                    2,
                    2,
                    2,
                    2,
                    2,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
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
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                ],
            ],
            schema=StructType(
                [
                    StructField("msisdn", StringType(), True),
                    StructField("weekstart", DateType(), True),
                    StructField(
                        "fea_txt_msg_ecommerce_shopping_incoming_count_01w",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_ecommerce_shopping_incoming_count_02w",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_ecommerce_shopping_incoming_count_03w",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_ecommerce_shopping_incoming_count_01m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_ecommerce_shopping_incoming_count_02m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_ecommerce_shopping_incoming_count_03m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_ecommerce_shopping_unique_senders_count_01w",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_ecommerce_shopping_unique_senders_count_02w",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_ecommerce_shopping_unique_senders_count_03w",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_ecommerce_shopping_unique_senders_count_01m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_ecommerce_shopping_unique_senders_count_02m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_ecommerce_shopping_unique_senders_count_03m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_government_incoming_count_01w", LongType(), True
                    ),
                    StructField(
                        "fea_txt_msg_government_incoming_count_02w", LongType(), True
                    ),
                    StructField(
                        "fea_txt_msg_government_incoming_count_03w", LongType(), True
                    ),
                    StructField(
                        "fea_txt_msg_government_incoming_count_01m", LongType(), True
                    ),
                    StructField(
                        "fea_txt_msg_government_incoming_count_02m", LongType(), True
                    ),
                    StructField(
                        "fea_txt_msg_government_incoming_count_03m", LongType(), True
                    ),
                    StructField(
                        "fea_txt_msg_government_tax_incoming_count_01w",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_government_tax_incoming_count_02w",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_government_tax_incoming_count_03w",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_government_tax_incoming_count_01m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_government_tax_incoming_count_02m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_government_tax_incoming_count_03m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_government_unique_senders_count_01w",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_government_unique_senders_count_02w",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_government_unique_senders_count_03w",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_government_unique_senders_count_01m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_government_unique_senders_count_02m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_government_unique_senders_count_03m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_messaging_incoming_count_01w", LongType(), True
                    ),
                    StructField(
                        "fea_txt_msg_messaging_incoming_count_02w", LongType(), True
                    ),
                    StructField(
                        "fea_txt_msg_messaging_incoming_count_03w", LongType(), True
                    ),
                    StructField(
                        "fea_txt_msg_messaging_incoming_count_01m", LongType(), True
                    ),
                    StructField(
                        "fea_txt_msg_messaging_incoming_count_02m", LongType(), True
                    ),
                    StructField(
                        "fea_txt_msg_messaging_incoming_count_03m", LongType(), True
                    ),
                    StructField(
                        "fea_txt_msg_messaging_unique_senders_count_01w",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_messaging_unique_senders_count_02w",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_messaging_unique_senders_count_03w",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_messaging_unique_senders_count_01m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_messaging_unique_senders_count_02m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_messaging_unique_senders_count_03m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_restaurants_incoming_count_01w", LongType(), True
                    ),
                    StructField(
                        "fea_txt_msg_restaurants_incoming_count_02w", LongType(), True
                    ),
                    StructField(
                        "fea_txt_msg_restaurants_incoming_count_03w", LongType(), True
                    ),
                    StructField(
                        "fea_txt_msg_restaurants_incoming_count_01m", LongType(), True
                    ),
                    StructField(
                        "fea_txt_msg_restaurants_incoming_count_02m", LongType(), True
                    ),
                    StructField(
                        "fea_txt_msg_restaurants_incoming_count_03m", LongType(), True
                    ),
                    StructField(
                        "fea_txt_msg_restaurants_unique_senders_count_01w",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_restaurants_unique_senders_count_02w",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_restaurants_unique_senders_count_03w",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_restaurants_unique_senders_count_01m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_restaurants_unique_senders_count_02m",
                        LongType(),
                        True,
                    ),
                    StructField(
                        "fea_txt_msg_restaurants_unique_senders_count_03m",
                        LongType(),
                        True,
                    ),
                    StructField("fea_txt_msg_kredivo_flag_01w", IntegerType(), True,),
                    StructField("fea_txt_msg_kredivo_flag_02w", IntegerType(), True,),
                    StructField("fea_txt_msg_kredivo_flag_03w", IntegerType(), True,),
                    StructField("fea_txt_msg_kredivo_flag_01m", IntegerType(), True,),
                    StructField("fea_txt_msg_kredivo_flag_02m", IntegerType(), True,),
                    StructField("fea_txt_msg_kredivo_flag_03m", IntegerType(), True,),
                    StructField(
                        "fea_txt_msg_kredivo_flag_all_time", IntegerType(), True,
                    ),
                ]
            ),
        )

        assert_df_frame_equal(actual_fea_df, expected_fea_df)
