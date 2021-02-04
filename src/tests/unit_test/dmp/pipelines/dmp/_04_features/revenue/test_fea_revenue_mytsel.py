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

from src.dmp.pipelines.dmp._04_features.revenue.fea_revenue_mytsel import (
    fea_revenue_mytsel,
)
from src.tests.pysaprk_df_equality import assert_df_frame_equal


@mock.patch(
    "src.dmp.pipelines.dmp._04_features.revenue.fea_revenue_mytsel.get_start_date",
    return_value=datetime.date(2018, 10, 10),
    autospec=True,
)
@mock.patch(
    "src.dmp.pipelines.dmp._04_features.revenue.fea_revenue_mytsel.get_end_date",
    return_value=datetime.date(2020, 2, 2),
    autospec=True,
)
class TestRevenueMyTSel:
    def test_fea_mytsel(
        self, mock_get_end_date, mock_get_start_date, spark_session: SparkSession
    ):
        df_revenue_mytsel_weekly = spark_session.createDataFrame(
            # fmt: off
            data=[
             #  ['msisdn', 'weekstart'               , 'product_name'   , 'service'  , 'total_rev', 'total_trx'],
                ['111'   , datetime.date(2020, 1, 6) , 'product_name_aa', 'service_a', 200.0      , 4          ],
                ['111'   , datetime.date(2020, 1, 6) , 'product_name_ab', 'service_a', 1000.0     , 2          ],
                ['111'   , datetime.date(2020, 1, 13), 'product_name_aa', 'service_a', 900.0      , 2          ],
                ['111'   , datetime.date(2020, 1, 13), 'product_name_ad', 'service_a', 950.0      , 2          ],
                ['111'   , datetime.date(2020, 1, 20), None             , None       , None       , None       ],
                ['111'   , datetime.date(2020, 1, 27), 'product_name_aa', 'service_a', 900.0      , 2          ],
                ['111'   , datetime.date(2020, 1, 27), 'product_name_ac', 'service_a', 900.0      , 2          ],

                ['222'   , datetime.date(2020, 1, 6) , 'product_name_aa', 'service_a', 100.0      , 4          ],
                ['222'   , datetime.date(2020, 1, 13), 'product_name_ac', 'service_a', 100.0      , 2          ],
                ['222'   , datetime.date(2020, 1, 13), None             , None       , None       , None       ],
                ['222'   , datetime.date(2020, 1, 20), 'product_name_bb', 'service_b', 100.0      , 2          ],
                ['222'   , datetime.date(2020, 1, 27), 'product_name_bc', 'service_b', 101.0      , 2          ],

                ['333'   , datetime.date(2020, 1, 6) , 'product_name_bb', 'service_b', 200.0      , 3          ],
                ['333'   , datetime.date(2020, 1, 13), None             , None       , None       , None       ],
                ['333'   , datetime.date(2020, 1, 20), None             , None       , None       , None       ],
                ['333'   , datetime.date(2020, 1, 27), None             , None       , None       , None       ],

                ['444'   , datetime.date(2020, 1, 6) , None             , None       , None       , None       ],
                ['444'   , datetime.date(2020, 1, 13), None             , None       , None       , None       ],
                ['444'   , datetime.date(2020, 1, 20), None             , None       , None       , None       ],
                ['444'   , datetime.date(2020, 1, 27), None             , None       , None       , None       ],

                # This weekstart should not come in output. Should be filtered
                ['111'   , datetime.date(2020, 2, 3) , 'product_name_ac', 'service_a', 900.0      , 2          ],
                ['222'   , datetime.date(2020, 2, 3) , None             , None       , None       , None       ],
                ['333'   , datetime.date(2020, 2, 3) , None             , None       , None       , None       ],
                ['444'   , datetime.date(2020, 2, 3) , None             , None       , None       , None       ],
            ],
            # fmt: on
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("weekstart", DateType(), False),
                    StructField("product_name", StringType(), True),
                    StructField("service", StringType(), True),
                    StructField("total_rev", DoubleType(), True),
                    StructField("total_trx", LongType(), True),
                ]
            ),
        )

        config_feature = {
            "fea_rev_mytsel_tot_rev": {
                "feature_name": "fea_rev_mytsel_tot_rev_{period_string}",
                "periods": ["01w", "02w", "03w"],
            },
            "fea_rev_mytsel_tot_trx": {
                "feature_name": "fea_rev_mytsel_tot_trx_{period_string}",
                "periods": ["01w", "02w", "03w"],
            },
            "fea_rev_mytsel_product_mode_by_trx": {
                "feature_name": "fea_rev_mytsel_product_mode_by_trx_{period_string}",
                "periods": ["01w", "02w", "03w", "01m"],
            },
            "fea_rev_mytsel_product_mode_by_rev": {
                "feature_name": "fea_rev_mytsel_product_mode_by_rev_{period_string}",
                "periods": ["01w", "02w", "03w", "01m", "02m"],
            },
            "fea_rev_mytsel_service_mode_by_trx": {
                "feature_name": "fea_rev_mytsel_service_mode_by_trx_{period_string}",
                "periods": ["01w", "02w", "03w"],
            },
            "fea_rev_mytsel_service_mode_by_rev": {
                "feature_name": "fea_rev_mytsel_service_mode_by_rev_{period_string}",
                "periods": ["01w", "02w", "03w"],
            },
        }

        actual_fea_revenue_mytsel = fea_revenue_mytsel(
            df_revenue_mytsel_weekly, config_feature, "all", []
        )
        expected_fea_revenue_mytsel = spark_session.createDataFrame(
            # fmt: off
            data = [
             #  ['msisdn', 'weekstart'               , 'fea_rev_mytsel_tot_rev_01w', 'fea_rev_mytsel_tot_rev_02w', 'fea_rev_mytsel_tot_rev_03w', 'fea_rev_mytsel_tot_trx_01w', 'fea_rev_mytsel_tot_trx_02w', 'fea_rev_mytsel_tot_trx_03w', 'fea_rev_mytsel_product_mode_by_rev_01w', 'fea_rev_mytsel_product_mode_by_rev_02w', 'fea_rev_mytsel_product_mode_by_rev_03w', 'fea_rev_mytsel_product_mode_by_rev_01m', 'fea_rev_mytsel_product_mode_by_rev_02m', 'fea_rev_mytsel_product_mode_by_trx_01w', 'fea_rev_mytsel_product_mode_by_trx_02w', 'fea_rev_mytsel_product_mode_by_trx_03w', 'fea_rev_mytsel_product_mode_by_trx_01m', 'fea_rev_mytsel_service_mode_by_rev_01w', 'fea_rev_mytsel_service_mode_by_rev_02w', 'fea_rev_mytsel_service_mode_by_rev_03w', 'fea_rev_mytsel_service_mode_by_trx_01w', 'fea_rev_mytsel_service_mode_by_trx_02w', 'fea_rev_mytsel_service_mode_by_trx_03w'],
                ['111'   , datetime.date(2020, 1, 6) , 1200.0                      , 1200.0                      , 1200.0                      , 6                           , 6                           , 6                           , 'product_name_ab'                       , 'product_name_ab'                       , 'product_name_ab'                       , 'product_name_ab'                       , 'product_name_ab'                       , 'product_name_aa'                       , 'product_name_aa'                       , 'product_name_aa'                       , 'product_name_aa'                       , 'service_a'                             , 'service_a'                             , 'service_a'                             , 'service_a'                             , 'service_a'                             , 'service_a'                             ],
                ['111'   , datetime.date(2020, 1, 13), 1850.0                      , 3050.0                      , 3050.0                      , 4                           , 10                          , 10                          , 'product_name_ad'                       , 'product_name_aa'                       , 'product_name_aa'                       , 'product_name_aa'                       , 'product_name_aa'                       , 'product_name_ad'                       , 'product_name_aa'                       , 'product_name_aa'                       , 'product_name_aa'                       , 'service_a'                             , 'service_a'                             , 'service_a'                             , 'service_a'                             , 'service_a'                             , 'service_a'                             ],
                ['111'   , datetime.date(2020, 1, 20), None                        , 1850.0                      , 3050.0                      , None                        , 4                           , 10                          , None                                    , 'product_name_ad'                       , 'product_name_aa'                       , 'product_name_aa'                       , 'product_name_aa'                       , None                                    , 'product_name_ad'                       , 'product_name_aa'                       , 'product_name_aa'                       , None                                    , 'service_a'                             , 'service_a'                             , None                                    , 'service_a'                             , 'service_a'                             ],
                ['111'   , datetime.date(2020, 1, 27), 1800.0                      , 1800.0                      , 3650.0                      , 4                           , 4                           , 8                           , 'product_name_aa'                       , 'product_name_aa'                       , 'product_name_aa'                       , 'product_name_aa'                       , 'product_name_aa'                       , 'product_name_aa'                       , 'product_name_aa'                       , 'product_name_aa'                       , 'product_name_aa'                       , 'service_a'                             , 'service_a'                             , 'service_a'                             , 'service_a'                             , 'service_a'                             , 'service_a'                             ],
                ['222'   , datetime.date(2020, 1, 6) , 100.0                       , 100.0                       , 100.0                       , 4                           , 4                           , 4                           , 'product_name_aa'                       , 'product_name_aa'                       , 'product_name_aa'                       , 'product_name_aa'                       , 'product_name_aa'                       , 'product_name_aa'                       , 'product_name_aa'                       , 'product_name_aa'                       , 'product_name_aa'                       , 'service_a'                             , 'service_a'                             , 'service_a'                             , 'service_a'                             , 'service_a'                             , 'service_a'                             ],
                ['222'   , datetime.date(2020, 1, 13), 100.0                       , 200.0                       , 200.0                       , 2                           , 6                           , 6                           , 'product_name_ac'                       , 'product_name_aa'                       , 'product_name_aa'                       , 'product_name_aa'                       , 'product_name_aa'                       , 'product_name_ac'                       , 'product_name_aa'                       , 'product_name_aa'                       , 'product_name_aa'                       , 'service_a'                             , 'service_a'                             , 'service_a'                             , 'service_a'                             , 'service_a'                             , 'service_a'                             ],
                ['222'   , datetime.date(2020, 1, 20), 100.0                       , 200.0                       , 300.0                       , 2                           , 4                           , 8                           , 'product_name_bb'                       , 'product_name_bb'                       , 'product_name_aa'                       , 'product_name_aa'                       , 'product_name_aa'                       , 'product_name_bb'                       , 'product_name_bb'                       , 'product_name_aa'                       , 'product_name_aa'                       , 'service_b'                             , 'service_b'                             , 'service_a'                             , 'service_b'                             , 'service_b'                             , 'service_a'                             ],
                ['222'   , datetime.date(2020, 1, 27), 101.0                       , 201.0                       , 301.0                       , 2                           , 4                           , 6                           , 'product_name_bc'                       , 'product_name_bc'                       , 'product_name_bc'                       , 'product_name_bc'                       , 'product_name_bc'                       , 'product_name_bc'                       , 'product_name_bc'                       , 'product_name_bc'                       , 'product_name_aa'                       , 'service_b'                             , 'service_b'                             , 'service_b'                             , 'service_b'                             , 'service_b'                             , 'service_b'                             ],
                ['333'   , datetime.date(2020, 1, 6) , 200.0                       , 200.0                       , 200.0                       , 3                           , 3                           , 3                           , 'product_name_bb'                       , 'product_name_bb'                       , 'product_name_bb'                       , 'product_name_bb'                       , 'product_name_bb'                       , 'product_name_bb'                       , 'product_name_bb'                       , 'product_name_bb'                       , 'product_name_bb'                       , 'service_b'                             , 'service_b'                             , 'service_b'                             , 'service_b'                             , 'service_b'                             , 'service_b'                             ],
                ['333'   , datetime.date(2020, 1, 13), None                        , 200.0                       , 200.0                       , None                        , 3                           , 3                           , None                                    , 'product_name_bb'                       , 'product_name_bb'                       , 'product_name_bb'                       , 'product_name_bb'                       , None                                    , 'product_name_bb'                       , 'product_name_bb'                       , 'product_name_bb'                       , None                                    , 'service_b'                             , 'service_b'                             , None                                    , 'service_b'                             , 'service_b'                             ],
                ['333'   , datetime.date(2020, 1, 20), None                        , None                        , 200.0                       , None                        , None                        , 3                           , None                                    , None                                    , 'product_name_bb'                       , 'product_name_bb'                       , 'product_name_bb'                       , None                                    , None                                    , 'product_name_bb'                       , 'product_name_bb'                       , None                                    , None                                    , 'service_b'                             , None                                    , None                                    , 'service_b'                             ],
                ['333'   , datetime.date(2020, 1, 27), None                        , None                        , None                        , None                        , None                        , None                        , None                                    , None                                    , None                                    , 'product_name_bb'                       , 'product_name_bb'                       , None                                    , None                                    , None                                    , 'product_name_bb'                       , None                                    , None                                    , None                                    , None                                    , None                                    , None                                    ],
                ['444'   , datetime.date(2020, 1, 6) , None                        , None                        , None                        , None                        , None                        , None                        , None                                    , None                                    , None                                    , None                                    , None                                    , None                                    , None                                    , None                                    , None                                    , None                                    , None                                    , None                                    , None                                    , None                                    , None                                    ],
                ['444'   , datetime.date(2020, 1, 13), None                        , None                        , None                        , None                        , None                        , None                        , None                                    , None                                    , None                                    , None                                    , None                                    , None                                    , None                                    , None                                    , None                                    , None                                    , None                                    , None                                    , None                                    , None                                    , None                                    ],
                ['444'   , datetime.date(2020, 1, 20), None                        , None                        , None                        , None                        , None                        , None                        , None                                    , None                                    , None                                    , None                                    , None                                    , None                                    , None                                    , None                                    , None                                    , None                                    , None                                    , None                                    , None                                    , None                                    , None                                    ],
                ['444'   , datetime.date(2020, 1, 27), None                        , None                        , None                        , None                        , None                        , None                        , None                                    , None                                    , None                                    , None                                    , None                                    , None                                    , None                                    , None                                    , None                                    , None                                    , None                                    , None                                    , None                                    , None                                    , None                                    ],
            ],
            # fmt: on
            schema=StructType(
                [
                    StructField("msisdn", StringType(), False),
                    StructField("weekstart", DateType(), False),
                    StructField("fea_rev_mytsel_tot_rev_01w", DoubleType(), True),
                    StructField("fea_rev_mytsel_tot_rev_02w", DoubleType(), True),
                    StructField("fea_rev_mytsel_tot_rev_03w", DoubleType(), True),
                    StructField("fea_rev_mytsel_tot_trx_01w", LongType(), True),
                    StructField("fea_rev_mytsel_tot_trx_02w", LongType(), True),
                    StructField("fea_rev_mytsel_tot_trx_03w", LongType(), True),
                    StructField(
                        "fea_rev_mytsel_product_mode_by_rev_01w", StringType(), True
                    ),
                    StructField(
                        "fea_rev_mytsel_product_mode_by_rev_02w", StringType(), True
                    ),
                    StructField(
                        "fea_rev_mytsel_product_mode_by_rev_03w", StringType(), True
                    ),
                    StructField(
                        "fea_rev_mytsel_product_mode_by_rev_01m", StringType(), True
                    ),
                    StructField(
                        "fea_rev_mytsel_product_mode_by_rev_02m", StringType(), True
                    ),
                    StructField(
                        "fea_rev_mytsel_product_mode_by_trx_01w", StringType(), True
                    ),
                    StructField(
                        "fea_rev_mytsel_product_mode_by_trx_02w", StringType(), True
                    ),
                    StructField(
                        "fea_rev_mytsel_product_mode_by_trx_03w", StringType(), True
                    ),
                    StructField(
                        "fea_rev_mytsel_product_mode_by_trx_01m", StringType(), True
                    ),
                    StructField(
                        "fea_rev_mytsel_service_mode_by_rev_01w", StringType(), True
                    ),
                    StructField(
                        "fea_rev_mytsel_service_mode_by_rev_02w", StringType(), True
                    ),
                    StructField(
                        "fea_rev_mytsel_service_mode_by_rev_03w", StringType(), True
                    ),
                    StructField(
                        "fea_rev_mytsel_service_mode_by_trx_01w", StringType(), True
                    ),
                    StructField(
                        "fea_rev_mytsel_service_mode_by_trx_02w", StringType(), True
                    ),
                    StructField(
                        "fea_rev_mytsel_service_mode_by_trx_03w", StringType(), True
                    ),
                ]
            ),
        )

        assert_df_frame_equal(actual_fea_revenue_mytsel, expected_fea_revenue_mytsel)
