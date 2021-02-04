import datetime
from unittest import mock

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.dmp.pipelines.dmp._04_features.recharge.fea_recharge_mytsel import (
    fea_recharge_mytsel,
)
from src.tests.pysaprk_df_equality import assert_df_frame_equal


@mock.patch(
    "src.dmp.pipelines.dmp._04_features.recharge.fea_recharge_mytsel.get_start_date",
    return_value=datetime.date(2020, 1, 6),
    autospec=True,
)
@mock.patch(
    "src.dmp.pipelines.dmp._04_features.recharge.fea_recharge_mytsel.get_end_date",
    return_value=datetime.date(2020, 10, 19),
    autospec=True,
)
class TestFeatureMyTselRecharge:
    def test_fea_recharge_mytsel(
        self, mock_get_end_date, mock_get_start_date, spark_session: SparkSession
    ):

        df_rech_mytsel_weekly = spark_session.createDataFrame(
            # fmt: off
            data=[
              # ['msisdn', 'weekstart'                , 'store_location', 'tot_trx_sum', 'tot_amt_sum'],
                ['111'   , datetime.date(2020, 9, 28) , 'CREDIT_CARD'   , 1            , 800.0        ],
                ['111'   , datetime.date(2020, 10, 5) , 'VA_BCA'        , 1            , 50.0         ],
                ['111'   , datetime.date(2020, 10, 5) , 'VA_BNI'        , 2            , 250.0        ],
                ['111'   , datetime.date(2020, 10, 12), '0'             , 1            , 100.0        ],
                ['111'   , datetime.date(2020, 10, 12), 'VA_BCA'        , 2            , 100.0        ],
                ['111'   , datetime.date(2020, 10, 12), 'VA_BNI'        , 1            , 150.0        ],
                ['111'   , datetime.date(2020, 10, 19), '0'             , 4            , 200.0        ],

                ['222'   , datetime.date(2020, 9, 28) , '0'             , 6            , 100.0        ],
                ['222'   , datetime.date(2020, 10, 5) , '0'             , 6            , 100.0        ],
                ['222'   , datetime.date(2020, 10, 5) , 'VA_BCA'        , 1            , 150.0        ],
                ['222'   , datetime.date(2020, 10, 12), '0'             , 3            , 150.0        ],
                ['222'   , datetime.date(2020, 10, 19), None            , None         , None         ],

                ['444'   , datetime.date(2020, 9, 28) , None            , None         , None         ],
                ['444'   , datetime.date(2020, 10, 5) , None            , None         , None         ],
                ['444'   , datetime.date(2020, 10, 12), None            , None         , None         ],
                ['444'   , datetime.date(2020, 10, 19), None            , None         , None         ],
            ],
            # fmt: on
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("weekstart", DateType(), False),
                    StructField("store_location", StringType(), True),
                    StructField("tot_trx_sum", LongType(), True),
                    StructField("tot_amt_sum", DoubleType(), True),
                ]
            ),
        )

        config_feature = {
            "fea_rech_mytsel_tot_amt_sum": {
                "feature_name": "fea_rech_mytsel_tot_amt_sum_{period_string}",
                "periods": ["01w", "02w", "03w", "01m", "02m", "03m"],
            },
            "fea_rech_mytsel_tot_trx_sum": {
                "feature_name": "fea_rech_mytsel_tot_trx_sum_{period_string}",
                "periods": ["01w", "02w", "03w", "01m", "02m", "03m"],
            },
            "fea_rech_mytsel_payment_mode_by_trx": {
                "feature_name": "fea_rech_mytsel_payment_mode_by_trx_{period_string}",
                "periods": ["01w", "02w", "03w", "01m", "02m", "03m"],
            },
            "fea_rech_mytsel_payment_mode_by_value": {
                "feature_name": "fea_rech_mytsel_payment_mode_by_value_{period_string}",
                "periods": ["01w", "02w", "03w", "01m", "02m", "03m"],
            },
        }

        actual_fea_df = fea_recharge_mytsel(
            df_rech_mytsel_weekly,
            config_feature,
            feature_mode="all",
            required_output_features=[],
        )

        expected_fea_df = spark_session.createDataFrame(
            # fmt: off
            data = [
              # ['msisdn', 'weekstart'                , 'fea_rech_mytsel_tot_amt_sum_01w', 'fea_rech_mytsel_tot_amt_sum_02w', 'fea_rech_mytsel_tot_amt_sum_03w', 'fea_rech_mytsel_tot_amt_sum_01m', 'fea_rech_mytsel_tot_amt_sum_02m', 'fea_rech_mytsel_tot_amt_sum_03m', 'fea_rech_mytsel_tot_trx_sum_01w', 'fea_rech_mytsel_tot_trx_sum_02w', 'fea_rech_mytsel_tot_trx_sum_03w', 'fea_rech_mytsel_tot_trx_sum_01m', 'fea_rech_mytsel_tot_trx_sum_02m', 'fea_rech_mytsel_tot_trx_sum_03m', 'fea_rech_mytsel_payment_mode_by_trx_01w', 'fea_rech_mytsel_payment_mode_by_trx_02w', 'fea_rech_mytsel_payment_mode_by_trx_03w', 'fea_rech_mytsel_payment_mode_by_trx_01m', 'fea_rech_mytsel_payment_mode_by_trx_02m', 'fea_rech_mytsel_payment_mode_by_trx_03m', 'fea_rech_mytsel_payment_mode_by_value_01w', 'fea_rech_mytsel_payment_mode_by_value_02w', 'fea_rech_mytsel_payment_mode_by_value_03w', 'fea_rech_mytsel_payment_mode_by_value_01m', 'fea_rech_mytsel_payment_mode_by_value_02m', 'fea_rech_mytsel_payment_mode_by_value_03m'],
                ['111'   , datetime.date(2020, 9, 28) , 800.0                            , 800.0                            , 800.0                            , 800.0                            , 800.0                            , 800.0                            , 1                                , 1                                , 1                                , 1                                , 1                                , 1                                , 'CREDIT_CARD'                            , 'CREDIT_CARD'                            , 'CREDIT_CARD'                            , 'CREDIT_CARD'                            , 'CREDIT_CARD'                            , 'CREDIT_CARD'                            , 'CREDIT_CARD'                              , 'CREDIT_CARD'                              , 'CREDIT_CARD'                              , 'CREDIT_CARD'                              , 'CREDIT_CARD'                              , 'CREDIT_CARD'                              ],
                ['111'   , datetime.date(2020, 10, 5) , 300.0                            , 1100.0                           , 1100.0                           , 1100.0                           , 1100.0                           , 1100.0                           , 3                                , 4                                , 4                                , 4                                , 4                                , 4                                , 'VA_BNI'                                 , 'VA_BNI'                                 , 'VA_BNI'                                 , 'VA_BNI'                                 , 'VA_BNI'                                 , 'VA_BNI'                                 , 'VA_BNI'                                   , 'CREDIT_CARD'                              , 'CREDIT_CARD'                              , 'CREDIT_CARD'                              , 'CREDIT_CARD'                              , 'CREDIT_CARD'                              ],
                ['111'   , datetime.date(2020, 10, 12), 350.0                            , 650.0                            , 1450.0                           , 1450.0                           , 1450.0                           , 1450.0                           , 4                                , 7                                , 8                                , 8                                , 8                                , 8                                , 'VA_BCA'                                 , 'VA_BNI'                                 , 'VA_BNI'                                 , 'VA_BNI'                                 , 'VA_BNI'                                 , 'VA_BNI'                                 , 'VA_BNI'                                   , 'VA_BNI'                                   , 'CREDIT_CARD'                              , 'CREDIT_CARD'                              , 'CREDIT_CARD'                              , 'CREDIT_CARD'                              ],
                ['111'   , datetime.date(2020, 10, 19), 200.0                            , 550.0                            , 850.0                            , 1650.0                           , 1650.0                           , 1650.0                           , 4                                , 8                                , 11                               , 12                               , 12                               , 12                               , '0'                                      , '0'                                      , '0'                                      , '0'                                      , '0'                                      , '0'                                      , '0'                                        , '0'                                        , 'VA_BNI'                                   , 'CREDIT_CARD'                              , 'CREDIT_CARD'                              , 'CREDIT_CARD'                              ],
                ['222'   , datetime.date(2020, 9, 28) , 100.0                            , 100.0                            , 100.0                            , 100.0                            , 100.0                            , 100.0                            , 6                                , 6                                , 6                                , 6                                , 6                                , 6                                , '0'                                      , '0'                                      , '0'                                      , '0'                                      , '0'                                      , '0'                                      , '0'                                        , '0'                                        , '0'                                        , '0'                                        , '0'                                        , '0'                                        ],
                ['222'   , datetime.date(2020, 10, 5) , 250.0                            , 350.0                            , 350.0                            , 350.0                            , 350.0                            , 350.0                            , 7                                , 13                               , 13                               , 13                               , 13                               , 13                               , '0'                                      , '0'                                      , '0'                                      , '0'                                      , '0'                                      , '0'                                      , 'VA_BCA'                                   , '0'                                        , '0'                                        , '0'                                        , '0'                                        , '0'                                        ],
                ['222'   , datetime.date(2020, 10, 12), 150.0                            , 400.0                            , 500.0                            , 500.0                            , 500.0                            , 500.0                            , 3                                , 10                               , 16                               , 16                               , 16                               , 16                               , '0'                                      , '0'                                      , '0'                                      , '0'                                      , '0'                                      , '0'                                      , '0'                                        , '0'                                        , '0'                                        , '0'                                        , '0'                                        , '0'                                        ],
                ['222'   , datetime.date(2020, 10, 19), None                             , 150.0                            , 400.0                            , 500.0                            , 500.0                            , 500.0                            , None                             , 3                                , 10                               , 16                               , 16                               , 16                               , '0'                                      , '0'                                      , '0'                                      , '0'                                      , '0'                                      , '0'                                      , '0'                                        , '0'                                        , '0'                                        , '0'                                        , '0'                                        , '0'                                        ],
                ['444'   , datetime.date(2020, 9, 28) , None                             , None                             , None                             , None                             , None                             , None                             , None                             , None                             , None                             , None                             , None                             , None                             , None                                     , None                                     , None                                     , None                                     , None                                     , None                                     , None                                       , None                                       , None                                       , None                                       , None                                       , None                                       ],
                ['444'   , datetime.date(2020, 10, 5) , None                             , None                             , None                             , None                             , None                             , None                             , None                             , None                             , None                             , None                             , None                             , None                             , None                                     , None                                     , None                                     , None                                     , None                                     , None                                     , None                                       , None                                       , None                                       , None                                       , None                                       , None                                       ],
                ['444'   , datetime.date(2020, 10, 12), None                             , None                             , None                             , None                             , None                             , None                             , None                             , None                             , None                             , None                             , None                             , None                             , None                                     , None                                     , None                                     , None                                     , None                                     , None                                     , None                                       , None                                       , None                                       , None                                       , None                                       , None                                       ],
                ['444'   , datetime.date(2020, 10, 19), None                             , None                             , None                             , None                             , None                             , None                             , None                             , None                             , None                             , None                             , None                             , None                             , None                                     , None                                     , None                                     , None                                     , None                                     , None                                     , None                                       , None                                       , None                                       , None                                       , None                                       , None                                       ],
            ],
            # fmt: on
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("weekstart", DateType(), False),
                    StructField("fea_rech_mytsel_tot_amt_sum_01w", DoubleType(), True),
                    StructField("fea_rech_mytsel_tot_amt_sum_02w", DoubleType(), True),
                    StructField("fea_rech_mytsel_tot_amt_sum_03w", DoubleType(), True),
                    StructField("fea_rech_mytsel_tot_amt_sum_01m", DoubleType(), True),
                    StructField("fea_rech_mytsel_tot_amt_sum_02m", DoubleType(), True),
                    StructField("fea_rech_mytsel_tot_amt_sum_03m", DoubleType(), True),
                    StructField("fea_rech_mytsel_tot_trx_sum_01w", LongType(), True),
                    StructField("fea_rech_mytsel_tot_trx_sum_02w", LongType(), True),
                    StructField("fea_rech_mytsel_tot_trx_sum_03w", LongType(), True),
                    StructField("fea_rech_mytsel_tot_trx_sum_01m", LongType(), True),
                    StructField("fea_rech_mytsel_tot_trx_sum_02m", LongType(), True),
                    StructField("fea_rech_mytsel_tot_trx_sum_03m", LongType(), True),
                    StructField(
                        "fea_rech_mytsel_payment_mode_by_trx_01w", StringType(), True
                    ),
                    StructField(
                        "fea_rech_mytsel_payment_mode_by_trx_02w", StringType(), True
                    ),
                    StructField(
                        "fea_rech_mytsel_payment_mode_by_trx_03w", StringType(), True
                    ),
                    StructField(
                        "fea_rech_mytsel_payment_mode_by_trx_01m", StringType(), True
                    ),
                    StructField(
                        "fea_rech_mytsel_payment_mode_by_trx_02m", StringType(), True
                    ),
                    StructField(
                        "fea_rech_mytsel_payment_mode_by_trx_03m", StringType(), True
                    ),
                    StructField(
                        "fea_rech_mytsel_payment_mode_by_value_01w", StringType(), True
                    ),
                    StructField(
                        "fea_rech_mytsel_payment_mode_by_value_02w", StringType(), True
                    ),
                    StructField(
                        "fea_rech_mytsel_payment_mode_by_value_03w", StringType(), True
                    ),
                    StructField(
                        "fea_rech_mytsel_payment_mode_by_value_01m", StringType(), True
                    ),
                    StructField(
                        "fea_rech_mytsel_payment_mode_by_value_02m", StringType(), True
                    ),
                    StructField(
                        "fea_rech_mytsel_payment_mode_by_value_03m", StringType(), True
                    ),
                ]
            ),
        )
        assert_df_frame_equal(actual_fea_df, expected_fea_df)
