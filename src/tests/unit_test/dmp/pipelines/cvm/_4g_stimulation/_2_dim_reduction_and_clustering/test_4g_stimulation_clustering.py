import datetime
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest
from prince import PCA
from pyspark.sql.types import (
    ArrayType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from sklearn.cluster import KMeans

from src.dmp.pipelines.cvm._4g_stimulation._1_costumer_segmentation.nodes import (
    add_month_to_master_table,
    concatenate_lac_ci,
    downsampling_master_table,
    join_wrapper,
    select_desired_weekstart,
    select_specific_partner_from_master_table,
    subselect_desired_columns,
)
from src.dmp.pipelines.cvm._4g_stimulation._2_dim_reduction_and_clustering._1_data_cleaning import (
    cast_decimals_to_float,
    pick_a_segment_and_month,
    remove_unit_of_analysis,
    select_df_to_score_or_train,
    specific_transformations,
    subset_features,
    to_pandas,
    transforming_columns,
)
from src.dmp.pipelines.cvm._4g_stimulation._2_dim_reduction_and_clustering._2_dim_reduction import (
    apply_PCA,
    numerical_categorical,
    transform_dimension_reduction,
)
from src.dmp.pipelines.cvm._4g_stimulation._2_dim_reduction_and_clustering._3_clustering import (
    concatenate_results_of_clustering,
    downsample,
    joining_kmeans_result_and_original_table,
    joining_prepaid_postpaid_features,
    joining_with_complete_master_table,
    kmeans_clustering_fit,
    kmeans_clustering_predict,
    run_statistics_profile,
)


@pytest.mark.skip()
def test_select_specific_partner_from_master_table_test(spark_session):
    # having partner in columns
    rech_df_schema_partner = StructType(
        [
            StructField("msisdn", StringType(), True),
            StructField("weekstart", StringType(), True),
            StructField("partner", ArrayType(StringType(), True), True),
            StructField("fea_market_name", StringType(), True),
            StructField("imeis", ArrayType(StringType(), True), True),
            StructField("fea_handset_make_01m", StringType(), True),
        ]
    )
    rech_df_partner = spark_session.createDataFrame(
        [
            (
                "A",
                "2020-01-27",
                ["internal-random-sample"],
                "Nokia 105 DS",
                ["78091145200", "0006847123"],
                "ASUSTek Computer Inc",
            ),
            (
                "B",
                "2020-01-27",
                ["internal_ucg_false"],
                "MDG2",
                ["8103522813"],
                "Xiaomi Communications Co Ltd",
            ),
            (
                "C",
                "2020-01-27",
                ["internal-random-sample"],
                "Samsung SM-J111F/DS",
                ["01809984498"],
                "Samsung Korea",
            ),
            (
                "D",
                "2020-01-01",
                ["kredivo"],
                "CPH1729",
                None,
                "Guangdong Oppo Mobile Telecommunications Corp Ltd",
            ),
            (
                "E",
                "2020-01-22",
                ["internal-random-sample"],
                "ASUS ZC554KL",
                ["840834525452"],
                "ASUSTek Computer Inc",
            ),
            (
                "F",
                "2020-01-07",
                ["kredivo-lookalike"],
                "Samsung SM-J730G/DS ",
                ["96082164452"],
                "Samsung Korea",
            ),
            (
                "G",
                "2020-01-12",
                ["internal-random-sample"],
                "MITO 388",
                ["0607027514"],
                "PT. Maju Express Indonesia",
            ),
        ],
        rech_df_schema_partner,
    )

    # not having partner in columns
    rech_df_schema_nopartner = StructType(
        [
            StructField("msisdn", StringType(), True),
            StructField("fea_handset_market_name", StringType(), True),
            StructField("fea_handset_type", StringType(), True),
            StructField("fea_handset_imeis", ArrayType(StringType(), True), True),
            StructField("fea_handset_make_01m", StringType(), True),
            StructField("fea_handset_make_02m", StringType(), True),
        ]
    )
    rech_df_nopartner = spark_session.createDataFrame(
        [
            ("A", None, None, ["55091591666"], None, None),
            ("B", None, None, None, None, None),
            ("C", None, None, None, None, None),
            ("D", None, None, None, None, None),
            ("E", "Samsung SM-G950U", "Smartphone", ["11090538958"], None, None),
            ("F", None, None, None, None, None),
            ("G", None, None, None, None, None),
        ],
        rech_df_schema_nopartner,
    )

    # lacci master table
    rech_df_lacci_schema = StructType(
        [
            StructField("lac_ci", StringType(), True),
            StructField("weekstart", DateType(), True),
            StructField("fea_network_2g_avail", StringType(), True),
            StructField("fea_network_3g_avail", StringType(), True),
            StructField("fea_network_azimuth", LongType(), True),
            StructField("fea_network_band", StringType(), True),
        ]
    )
    rech_df_lacci = spark_session.createDataFrame(
        [
            ("lac_ci1", datetime.date(2020, 1, 27), "0", "0", 0, "F3"),
            ("lac_ci1", datetime.date(2020, 1, 27), "0", "0", 25, "F3"),
            ("lac_ci1", datetime.date(2020, 1, 27), "0", "0", 0, "F1"),
            ("lac_ci1", datetime.date(2020, 1, 27), "0", "0", 90, "F1"),
            ("lac_ci1", datetime.date(2020, 1, 27), "0", "0", 320, "LTE1800"),
            ("lac_ci1", datetime.date(2020, 1, 27), "0", "0", 30, "DCS"),
            ("lac_ci1", datetime.date(2019, 1, 20), "0", "0", 120, "F2"),
        ],
        rech_df_lacci_schema,
    )

    # lte maps
    rech_df_schema_lte = StructType(
        [
            StructField("msisdn", StringType(), True),
            StructField("segment", StringType(), True),
            StructField("month", StringType(), True),
            StructField("device4g", StringType(), True),
            StructField("usim", StringType(), True),
            StructField("sdn", StringType(), True),
            StructField("payload_4g_kb", StringType(), True),
            StructField("quadrant", StringType(), True),
        ]
    )
    rech_df_lte = spark_session.createDataFrame(
        [
            ("A", "SEGMENT_2", "202001", "Y", "Y", "N", 0.0, "USIM-LTE"),
            ("B", "SEGMENT_1", "201911", "Y", "Y", "Y", 10933698.0, "USIM-LTE"),
            ("C", "SEGMENT_2", "202001", "Y", "Y", "Y", 9050384.0, "USIM-LTE"),
            ("D", "SEGMENT_2", "202001", "Y", "Y", "Y", 27457729.0, "USIM-LTE"),
            ("E", "SEGMENT_2", "202001", "Y", "Y", "N", 0.0, "USIM-LTE"),
            ("F", "SEGMENT_2", "202001", "N", "Y", "N", 0.0, "USIM-NONLTE"),
            ("G", "SEGMENT_2", "201911", "Y", "Y", "Y", 30043334.0, "USIM-LTE"),
        ],
        rech_df_schema_lte,
    )

    # cb prepaid postpaid
    rech_df_schema_cb = StructType(
        [
            StructField("msisdn", StringType(), True),
            StructField("lac", StringType(), True),
            StructField("ci", StringType(), True),
            StructField("month", StringType(), True),
            StructField("poin", IntegerType(), True),
            StructField("gender", StringType(), True),
        ]
    )
    rech_df_cb = spark_session.createDataFrame(
        [
            ("A", "210", "450", "202001", 96, "MALE"),
            ("B", "211", "451", "201911", 2911, "FEMALE"),
            ("C", "212", "452", "202001", 92, None),
            ("D", None, None, "202001", 76, None),
            ("E", "213", "453", "202001", 1386, "MALE"),
            ("F", "214", "454", "202001", 86, "MALE"),
            ("G", "215", "455", "201911", 735, "MALE"),
        ],
        rech_df_schema_cb,
    )

    # imei db
    rech_df_schema_imei = StructType(
        [
            StructField("msisdn", StringType(), True),
            StructField("month", StringType(), True),
            StructField("imei", StringType(), True),
            StructField("tac", StringType(), True),
            StructField("manufacture", StringType(), True),
            StructField("design_type", StringType(), True),
            StructField("device_type", StringType(), True),
            StructField("data_capable", StringType(), True),
            StructField("os", StringType(), True),
            StructField("network", StringType(), True),
            StructField("multisim", StringType(), True),
            StructField("volte", StringType(), True),
            StructField("flag", StringType(), True),
        ]
    )
    rech_df_imei = spark_session.createDataFrame(
        [
            (
                "A",
                "202001",
                "0510706679",
                "35446510",
                "SAMSUNG KOREA",
                "GALAXY A50",
                "SMARTPHONE",
                "Y",
                "ANDROID",
                "4G",
                "Y",
                "N",
                "1.SINGLE",
            ),
            (
                "B",
                "202001",
                "0700003934",
                "01321700",
                "APPLE INC",
                "APPLE IPAD (A1430)",
                "TABLET",
                "Y",
                "IOS",
                "4G",
                "N",
                "N",
                "1.SINGLE",
            ),
            (
                "C",
                "202001",
                "52107270262",
                "352452107",
                "SAMSUNG KOREA",
                "SAMSUNG SM-J500G/DS",
                "SMARTPHONE",
                "Y",
                "ANDROID",
                "4G",
                "Y",
                "Y",
                "1.SINGLE",
            ),
            (
                "D",
                "202001",
                "0309048041",
                "35512309",
                "SAMSUNG KOREA",
                "GALAXY A8+ SM-A730F/DS",
                "SMARTPHONE",
                "Y",
                "ANDROID",
                "4G",
                "N",
                "N",
                "1.SINGLE",
            ),
            (
                "E",
                "202001",
                "0207096301",
                "35720207",
                "SAMSUNG KOREA",
                "SAMSUNG SM-J510FN/DS",
                "SMARTPHONE",
                "Y",
                "ANDROID",
                "4G",
                "Y",
                "N",
                "1.SINGLE",
            ),
            (
                "F",
                "202001",
                "0003781937",
                "86463003",
                "ONEPLUS TECHNOLOGY (SHENZHEN) CO LTD",
                "ONEPLUS 5",
                "SMARTPHONE",
                "Y",
                "ANDROID",
                "4G",
                "N",
                "Y",
                "1.SINGLE",
            ),
            (
                "G",
                "202001",
                "0204228306",
                "86593204",
                "XIAOMI COMMUNICATIONS CO LTD",
                "REDMI NOTE 8 PRO",
                "SMARTPHONE",
                "Y",
                "ANDROID",
                "4G",
                "Y",
                "N",
                "1.SINGLE",
            ),
        ],
        rech_df_schema_imei,
    )

    # partner
    expected_out_partner = [
        [
            "A",
            "2020-01-27",
            ["internal-random-sample"],
            "Nokia 105 DS",
            ["78091145200", "0006847123"],
            "ASUSTek Computer Inc",
        ],
        [
            "C",
            "2020-01-27",
            ["internal-random-sample"],
            "Samsung SM-J111F/DS",
            ["01809984498"],
            "Samsung Korea",
        ],
        [
            "E",
            "2020-01-22",
            ["internal-random-sample"],
            "ASUS ZC554KL",
            ["840834525452"],
            "ASUSTek Computer Inc",
        ],
        [
            "G",
            "2020-01-12",
            ["internal-random-sample"],
            "MITO 388",
            ["0607027514"],
            "PT. Maju Express Indonesia",
        ],
    ]
    out_partner = select_specific_partner_from_master_table(
        rech_df_partner, True, "internal-random-sample", "2020-01-27"
    )
    out_partner_compared = [
        [i[0], i[1], i[2], i[3], i[4], i[5]] for i in out_partner.collect()
    ]
    assert sorted(out_partner_compared) == sorted(expected_out_partner)

    # nopartner
    expected_out_nopartner = [
        ["A", None, None, ["55091591666"], None, None, "2020-01-27"],
        ["B", None, None, None, None, None, "2020-01-27"],
        ["C", None, None, None, None, None, "2020-01-27"],
        ["D", None, None, None, None, None, "2020-01-27"],
        [
            "E",
            "Samsung SM-G950U",
            "Smartphone",
            ["11090538958"],
            None,
            None,
            "2020-01-27",
        ],
        ["F", None, None, None, None, None, "2020-01-27"],
        ["G", None, None, None, None, None, "2020-01-27"],
    ]
    out_nopartner = select_specific_partner_from_master_table(
        rech_df_nopartner, False, "internal-random-sample", "2020-01-27"
    )
    out_nopartner_compared = [
        [i[0], i[1], i[2], i[3], i[4], i[5], i[6]] for i in out_nopartner.collect()
    ]
    assert sorted(out_nopartner_compared) == sorted(expected_out_nopartner)

    # add month to master table for both(partner and nopartner)
    # partner
    out_partner_month = add_month_to_master_table(out_partner)
    out_partner_month_compared = [
        [i[0], i[1], i[2], i[3], i[4], i[5], i[6]] for i in out_partner_month.collect()
    ]
    expected_out_partner_month = [
        [
            "A",
            "2020-01-27",
            ["internal-random-sample"],
            "Nokia 105 DS",
            ["78091145200", "0006847123"],
            "ASUSTek Computer Inc",
            "202001",
        ],
        [
            "C",
            "2020-01-27",
            ["internal-random-sample"],
            "Samsung SM-J111F/DS",
            ["01809984498"],
            "Samsung Korea",
            "202001",
        ],
        [
            "E",
            "2020-01-22",
            ["internal-random-sample"],
            "ASUS ZC554KL",
            ["840834525452"],
            "ASUSTek Computer Inc",
            "202001",
        ],
        [
            "G",
            "2020-01-12",
            ["internal-random-sample"],
            "MITO 388",
            ["0607027514"],
            "PT. Maju Express Indonesia",
            "202001",
        ],
    ]
    assert sorted(expected_out_partner_month) == sorted(out_partner_month_compared)
    # no partner
    out_nopartner_month = add_month_to_master_table(out_nopartner)
    out_nopartner_month_compared = [
        [i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7]]
        for i in out_nopartner_month.collect()
    ]
    expected_out_nopartner_month = [
        ["A", None, None, ["55091591666"], None, None, "2020-01-27", "202001"],
        ["B", None, None, None, None, None, "2020-01-27", "202001"],
        ["C", None, None, None, None, None, "2020-01-27", "202001"],
        ["D", None, None, None, None, None, "2020-01-27", "202001"],
        [
            "E",
            "Samsung SM-G950U",
            "Smartphone",
            ["11090538958"],
            None,
            None,
            "2020-01-27",
            "202001",
        ],
        ["F", None, None, None, None, None, "2020-01-27", "202001"],
        ["G", None, None, None, None, None, "2020-01-27", "202001"],
    ]
    assert sorted(expected_out_nopartner_month) == sorted(out_nopartner_month_compared)

    # select desired weeks for both(partner and nopartner)
    out_partner_dws, out_lacci_dws = select_desired_weekstart(
        "2020-01-27", out_partner_month, rech_df_lacci
    )
    expected_out_partner_dws = [
        [
            "A",
            "2020-01-27",
            ["internal-random-sample"],
            "Nokia 105 DS",
            ["78091145200", "0006847123"],
            "ASUSTek Computer Inc",
            "202001",
        ],
        [
            "C",
            "2020-01-27",
            ["internal-random-sample"],
            "Samsung SM-J111F/DS",
            ["01809984498"],
            "Samsung Korea",
            "202001",
        ],
    ]
    assert sorted(expected_out_partner_dws) == sorted(
        [[i[0], i[1], i[2], i[3], i[4], i[5], i[6]] for i in out_partner_dws.collect()]
    )

    out_nopartner_dws, out_lacci_dws = select_desired_weekstart(
        "2020-01-27", out_nopartner_month, rech_df_lacci
    )
    expected_out_nopartner_dws = [
        ["A", None, None, ["55091591666"], None, None, "2020-01-27", "202001"],
        ["B", None, None, None, None, None, "2020-01-27", "202001"],
        ["C", None, None, None, None, None, "2020-01-27", "202001"],
        ["D", None, None, None, None, None, "2020-01-27", "202001"],
        [
            "E",
            "Samsung SM-G950U",
            "Smartphone",
            ["11090538958"],
            None,
            None,
            "2020-01-27",
            "202001",
        ],
        ["F", None, None, None, None, None, "2020-01-27", "202001"],
        ["G", None, None, None, None, None, "2020-01-27", "202001"],
    ]
    assert sorted(expected_out_nopartner_dws) == sorted(
        [
            [i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7]]
            for i in out_nopartner_dws.collect()
        ]
    )

    expected_out_lacci_dws = [
        ["lac_ci1", datetime.date(2020, 1, 27), "0", "0", 0, "F3"],
        ["lac_ci1", datetime.date(2020, 1, 27), "0", "0", 25, "F3"],
        ["lac_ci1", datetime.date(2020, 1, 27), "0", "0", 0, "F1"],
        ["lac_ci1", datetime.date(2020, 1, 27), "0", "0", 90, "F1"],
        ["lac_ci1", datetime.date(2020, 1, 27), "0", "0", 320, "LTE1800"],
        ["lac_ci1", datetime.date(2020, 1, 27), "0", "0", 30, "DCS"],
    ]
    assert sorted(expected_out_lacci_dws) == sorted(
        [[i[0], i[1], i[2], i[3], i[4], i[5]] for i in out_lacci_dws.collect()]
    )

    # select desired columns
    lte_map_cols = [
        "msisdn",
        "segment",
        "month",
        "device4g",
        "usim",
        "sdn",
        "payload_4g_kb",
    ]
    cb_prepaid_postpaid_cols = ["msisdn", "lac", "ci", "month"]
    imeidb_cols = [
        "msisdn",
        "month",
        "imei",
        "tac",
        "manufacture",
        "design_type",
        "device_type",
        "data_capable",
        "os",
        "network",
        "multisim",
        "volte",
    ]
    out_lte, out_cb, out_imei = subselect_desired_columns(
        rech_df_lte,
        lte_map_cols,
        rech_df_cb,
        cb_prepaid_postpaid_cols,
        rech_df_imei,
        imeidb_cols,
    )
    expected_out_lte = [
        ["A", "SEGMENT_2", "202001", "Y", "Y", "N", "0.0"],
        ["B", "SEGMENT_1", "201911", "Y", "Y", "Y", "1.0933698E7"],
        ["C", "SEGMENT_2", "202001", "Y", "Y", "Y", "9050384.0"],
        ["D", "SEGMENT_2", "202001", "Y", "Y", "Y", "2.7457729E7"],
        ["E", "SEGMENT_2", "202001", "Y", "Y", "N", "0.0"],
        ["F", "SEGMENT_2", "202001", "N", "Y", "N", "0.0"],
        ["G", "SEGMENT_2", "201911", "Y", "Y", "Y", "3.0043334E7"],
    ]
    assert sorted(expected_out_lte) == sorted(
        [[i[0], i[1], i[2], i[3], i[4], i[5], i[6]] for i in out_lte.collect()]
    )

    expected_out_cb = [
        ["A", "210", "450", "202001"],
        ["B", "211", "451", "201911"],
        ["C", "212", "452", "202001"],
        ["D", None, None, "202001"],
        ["E", "213", "453", "202001"],
        ["F", "214", "454", "202001"],
        ["G", "215", "455", "201911"],
    ]
    assert sorted(expected_out_cb) == sorted(
        [[i[0], i[1], i[2], i[3]] for i in out_cb.collect()]
    )

    expected_out_imei = [
        [
            "A",
            "202001",
            "0510706679",
            "35446510",
            "SAMSUNG KOREA",
            "GALAXY A50",
            "SMARTPHONE",
            "Y",
            "ANDROID",
            "4G",
            "Y",
            "N",
        ],
        [
            "B",
            "202001",
            "0700003934",
            "01321700",
            "APPLE INC",
            "APPLE IPAD (A1430)",
            "TABLET",
            "Y",
            "IOS",
            "4G",
            "N",
            "N",
        ],
        [
            "C",
            "202001",
            "52107270262",
            "352452107",
            "SAMSUNG KOREA",
            "SAMSUNG SM-J500G/DS",
            "SMARTPHONE",
            "Y",
            "ANDROID",
            "4G",
            "Y",
            "Y",
        ],
        [
            "D",
            "202001",
            "0309048041",
            "35512309",
            "SAMSUNG KOREA",
            "GALAXY A8+ SM-A730F/DS",
            "SMARTPHONE",
            "Y",
            "ANDROID",
            "4G",
            "N",
            "N",
        ],
        [
            "E",
            "202001",
            "0207096301",
            "35720207",
            "SAMSUNG KOREA",
            "SAMSUNG SM-J510FN/DS",
            "SMARTPHONE",
            "Y",
            "ANDROID",
            "4G",
            "Y",
            "N",
        ],
        [
            "F",
            "202001",
            "0003781937",
            "86463003",
            "ONEPLUS TECHNOLOGY (SHENZHEN) CO LTD",
            "ONEPLUS 5",
            "SMARTPHONE",
            "Y",
            "ANDROID",
            "4G",
            "N",
            "Y",
        ],
        [
            "G",
            "202001",
            "0204228306",
            "86593204",
            "XIAOMI COMMUNICATIONS CO LTD",
            "REDMI NOTE 8 PRO",
            "SMARTPHONE",
            "Y",
            "ANDROID",
            "4G",
            "Y",
            "N",
        ],
    ]
    assert sorted(expected_out_imei) == sorted(
        [
            [i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], i[10], i[11]]
            for i in out_imei.collect()
        ]
    )

    # join wrapper between master and lte
    out_join_partner = join_wrapper(out_partner_dws, out_lte)
    expected_join_partner = [
        [
            "A",
            "202001",
            "2020-01-27",
            ["internal-random-sample"],
            "Nokia 105 DS",
            ["78091145200", "0006847123"],
            "ASUSTek Computer Inc",
            "SEGMENT_2",
            "Y",
            "Y",
            "N",
            "0.0",
        ],
        [
            "C",
            "202001",
            "2020-01-27",
            ["internal-random-sample"],
            "Samsung SM-J111F/DS",
            ["01809984498"],
            "Samsung Korea",
            "SEGMENT_2",
            "Y",
            "Y",
            "Y",
            "9050384.0",
        ],
    ]

    out_join_nopartner = join_wrapper(out_nopartner_dws, out_lte)
    assert sorted(expected_join_partner) == sorted(
        [
            [i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], i[10], i[11]]
            for i in out_join_partner.collect()
        ]
    )
    expected_join_nopartner = [
        [
            "A",
            "202001",
            None,
            None,
            ["55091591666"],
            None,
            None,
            "2020-01-27",
            "SEGMENT_2",
            "Y",
            "Y",
            "N",
            "0.0",
        ],
        [
            "E",
            "202001",
            "Samsung SM-G950U",
            "Smartphone",
            ["11090538958"],
            None,
            None,
            "2020-01-27",
            "SEGMENT_2",
            "Y",
            "Y",
            "N",
            "0.0",
        ],
        [
            "F",
            "202001",
            None,
            None,
            None,
            None,
            None,
            "2020-01-27",
            "SEGMENT_2",
            "N",
            "Y",
            "N",
            "0.0",
        ],
        [
            "D",
            "202001",
            None,
            None,
            None,
            None,
            None,
            "2020-01-27",
            "SEGMENT_2",
            "Y",
            "Y",
            "Y",
            "2.7457729E7",
        ],
        [
            "C",
            "202001",
            None,
            None,
            None,
            None,
            None,
            "2020-01-27",
            "SEGMENT_2",
            "Y",
            "Y",
            "Y",
            "9050384.0",
        ],
    ]
    assert sorted(expected_join_nopartner) == sorted(
        [
            [
                i[0],
                i[1],
                i[2],
                i[3],
                i[4],
                i[5],
                i[6],
                i[7],
                i[8],
                i[9],
                i[10],
                i[11],
                i[12],
            ]
            for i in out_join_nopartner.collect()
        ]
    )

    # concat lac ci on cb prepaid postpaid
    out_cb_ = concatenate_lac_ci(out_cb)
    expected_cb_ = [
        ["A", "210", "450", "202001", "210-450"],
        ["B", "211", "451", "201911", "211-451"],
        ["C", "212", "452", "202001", "212-452"],
        ["D", None, None, "202001", None],
        ["E", "213", "453", "202001", "213-453"],
        ["F", "214", "454", "202001", "214-454"],
        ["G", "215", "455", "201911", "215-455"],
    ]
    assert sorted(expected_cb_) == sorted(
        [[i[0], i[1], i[2], i[3], i[4]] for i in out_cb_.collect()]
    )

    # join wrapper between master and cb
    out_join_partner_ = join_wrapper(out_join_partner, out_cb_)
    expected_join_partner_ = [
        [
            "A",
            "202001",
            "2020-01-27",
            ["internal-random-sample"],
            "Nokia 105 DS",
            ["78091145200", "0006847123"],
            "ASUSTek Computer Inc",
            "SEGMENT_2",
            "Y",
            "Y",
            "N",
            "0.0",
            "210",
            "450",
            "210-450",
        ],
        [
            "C",
            "202001",
            "2020-01-27",
            ["internal-random-sample"],
            "Samsung SM-J111F/DS",
            ["01809984498"],
            "Samsung Korea",
            "SEGMENT_2",
            "Y",
            "Y",
            "Y",
            "9050384.0",
            "212",
            "452",
            "212-452",
        ],
    ]
    assert sorted(expected_join_partner_) == sorted(
        [
            [
                i[0],
                i[1],
                i[2],
                i[3],
                i[4],
                i[5],
                i[6],
                i[7],
                i[8],
                i[9],
                i[10],
                i[11],
                i[12],
                i[13],
                i[14],
            ]
            for i in out_join_partner_.collect()
        ]
    )

    out_join_nopartner_ = join_wrapper(out_join_nopartner, out_cb_)
    expected_join_nopartner_ = [
        [
            "A",
            "202001",
            None,
            None,
            ["55091591666"],
            None,
            None,
            "2020-01-27",
            "SEGMENT_2",
            "Y",
            "Y",
            "N",
            "0.0",
            "210",
            "450",
            "210-450",
        ],
        [
            "E",
            "202001",
            "Samsung SM-G950U",
            "Smartphone",
            ["11090538958"],
            None,
            None,
            "2020-01-27",
            "SEGMENT_2",
            "Y",
            "Y",
            "N",
            "0.0",
            "213",
            "453",
            "213-453",
        ],
        [
            "F",
            "202001",
            None,
            None,
            None,
            None,
            None,
            "2020-01-27",
            "SEGMENT_2",
            "N",
            "Y",
            "N",
            "0.0",
            "214",
            "454",
            "214-454",
        ],
        [
            "D",
            "202001",
            None,
            None,
            None,
            None,
            None,
            "2020-01-27",
            "SEGMENT_2",
            "Y",
            "Y",
            "Y",
            "2.7457729E7",
            None,
            None,
            None,
        ],
        [
            "C",
            "202001",
            None,
            None,
            None,
            None,
            None,
            "2020-01-27",
            "SEGMENT_2",
            "Y",
            "Y",
            "Y",
            "9050384.0",
            "212",
            "452",
            "212-452",
        ],
    ]
    assert sorted(expected_join_nopartner_) == sorted(
        [
            [
                i[0],
                i[1],
                i[2],
                i[3],
                i[4],
                i[5],
                i[6],
                i[7],
                i[8],
                i[9],
                i[10],
                i[11],
                i[12],
                i[13],
                i[14],
                i[15],
            ]
            for i in out_join_nopartner_.collect()
        ]
    )

    # join wrapper between master and imei
    out_join_partner_1 = join_wrapper(out_join_partner_, out_imei)
    expected_join_partner_1 = [
        [
            "A",
            "202001",
            "2020-01-27",
            ["internal-random-sample"],
            "Nokia 105 DS",
            ["78091145200", "0006847123"],
            "ASUSTek Computer Inc",
            "SEGMENT_2",
            "Y",
            "Y",
            "N",
            "0.0",
            "210",
            "450",
            "210-450",
            "0510706679",
            "35446510",
            "SAMSUNG KOREA",
            "GALAXY A50",
            "SMARTPHONE",
            "Y",
            "ANDROID",
            "4G",
            "Y",
            "N",
        ],
        [
            "C",
            "202001",
            "2020-01-27",
            ["internal-random-sample"],
            "Samsung SM-J111F/DS",
            ["01809984498"],
            "Samsung Korea",
            "SEGMENT_2",
            "Y",
            "Y",
            "Y",
            "9050384.0",
            "212",
            "452",
            "212-452",
            "52107270262",
            "352452107",
            "SAMSUNG KOREA",
            "SAMSUNG SM-J500G/DS",
            "SMARTPHONE",
            "Y",
            "ANDROID",
            "4G",
            "Y",
            "Y",
        ],
    ]
    assert sorted(expected_join_partner_1) == sorted(
        [
            [
                i[0],
                i[1],
                i[2],
                i[3],
                i[4],
                i[5],
                i[6],
                i[7],
                i[8],
                i[9],
                i[10],
                i[11],
                i[12],
                i[13],
                i[14],
                i[15],
                i[16],
                i[17],
                i[18],
                i[19],
                i[20],
                i[21],
                i[22],
                i[23],
                i[24],
            ]
            for i in out_join_partner_1.collect()
        ]
    )

    out_join_nopartner_1 = join_wrapper(out_join_nopartner_, out_imei)
    expected_join_nopartner_1 = [
        [
            "A",
            "202001",
            None,
            None,
            ["55091591666"],
            None,
            None,
            "2020-01-27",
            "SEGMENT_2",
            "Y",
            "Y",
            "N",
            "0.0",
            "210",
            "450",
            "210-450",
            "0510706679",
            "35446510",
            "SAMSUNG KOREA",
            "GALAXY A50",
            "SMARTPHONE",
            "Y",
            "ANDROID",
            "4G",
            "Y",
            "N",
        ],
        [
            "E",
            "202001",
            "Samsung SM-G950U",
            "Smartphone",
            ["11090538958"],
            None,
            None,
            "2020-01-27",
            "SEGMENT_2",
            "Y",
            "Y",
            "N",
            "0.0",
            "213",
            "453",
            "213-453",
            "0207096301",
            "35720207",
            "SAMSUNG KOREA",
            "SAMSUNG SM-J510FN/DS",
            "SMARTPHONE",
            "Y",
            "ANDROID",
            "4G",
            "Y",
            "N",
        ],
        [
            "F",
            "202001",
            None,
            None,
            None,
            None,
            None,
            "2020-01-27",
            "SEGMENT_2",
            "N",
            "Y",
            "N",
            "0.0",
            "214",
            "454",
            "214-454",
            "0003781937",
            "86463003",
            "ONEPLUS TECHNOLOGY (SHENZHEN) CO LTD",
            "ONEPLUS 5",
            "SMARTPHONE",
            "Y",
            "ANDROID",
            "4G",
            "N",
            "Y",
        ],
        [
            "D",
            "202001",
            None,
            None,
            None,
            None,
            None,
            "2020-01-27",
            "SEGMENT_2",
            "Y",
            "Y",
            "Y",
            "2.7457729E7",
            None,
            None,
            None,
            "0309048041",
            "35512309",
            "SAMSUNG KOREA",
            "GALAXY A8+ SM-A730F/DS",
            "SMARTPHONE",
            "Y",
            "ANDROID",
            "4G",
            "N",
            "N",
        ],
        [
            "C",
            "202001",
            None,
            None,
            None,
            None,
            None,
            "2020-01-27",
            "SEGMENT_2",
            "Y",
            "Y",
            "Y",
            "9050384.0",
            "212",
            "452",
            "212-452",
            "52107270262",
            "352452107",
            "SAMSUNG KOREA",
            "SAMSUNG SM-J500G/DS",
            "SMARTPHONE",
            "Y",
            "ANDROID",
            "4G",
            "Y",
            "Y",
        ],
    ]
    assert sorted(expected_join_nopartner_1) == sorted(
        [
            [
                i[0],
                i[1],
                i[2],
                i[3],
                i[4],
                i[5],
                i[6],
                i[7],
                i[8],
                i[9],
                i[10],
                i[11],
                i[12],
                i[13],
                i[14],
                i[15],
                i[16],
                i[17],
                i[18],
                i[19],
                i[20],
                i[21],
                i[22],
                i[23],
                i[24],
                i[25],
            ]
            for i in out_join_nopartner_1.collect()
        ]
    )

    # join wrapper between master and lac_ci
    out_join_partner_final = join_wrapper(
        out_join_partner_1, out_lacci_dws, cols=["lac_ci", "weekstart"], how="left"
    )
    expected_join_partner_final = [
        [
            "212-452",
            "2020-01-27",
            "C",
            "202001",
            ["internal-random-sample"],
            "Samsung SM-J111F/DS",
            ["01809984498"],
            "Samsung Korea",
            "SEGMENT_2",
            "Y",
            "Y",
            "Y",
            "9050384.0",
            "212",
            "452",
            "52107270262",
            "352452107",
            "SAMSUNG KOREA",
            "SAMSUNG SM-J500G/DS",
            "SMARTPHONE",
            "Y",
            "ANDROID",
            "4G",
            "Y",
            "Y",
            None,
            None,
            None,
            None,
        ],
        [
            "210-450",
            "2020-01-27",
            "A",
            "202001",
            ["internal-random-sample"],
            "Nokia 105 DS",
            ["78091145200", "0006847123"],
            "ASUSTek Computer Inc",
            "SEGMENT_2",
            "Y",
            "Y",
            "N",
            "0.0",
            "210",
            "450",
            "0510706679",
            "35446510",
            "SAMSUNG KOREA",
            "GALAXY A50",
            "SMARTPHONE",
            "Y",
            "ANDROID",
            "4G",
            "Y",
            "N",
            None,
            None,
            None,
            None,
        ],
    ]
    assert sorted(expected_join_partner_final) == sorted(
        [
            [
                i[0],
                i[1],
                i[2],
                i[3],
                i[4],
                i[5],
                i[6],
                i[7],
                i[8],
                i[9],
                i[10],
                i[11],
                i[12],
                i[13],
                i[14],
                i[15],
                i[16],
                i[17],
                i[18],
                i[19],
                i[20],
                i[21],
                i[22],
                i[23],
                i[24],
                i[25],
                i[26],
                i[27],
                i[28],
            ]
            for i in out_join_partner_final.collect()
        ]
    )

    out_join_nopartner_final = join_wrapper(
        out_join_nopartner_1, out_lacci_dws, cols=["lac_ci", "weekstart"], how="left"
    )
    expected_join_nopartner_final = [
        [
            "210-450",
            "2020-01-27",
            "A",
            "202001",
            None,
            None,
            ["55091591666"],
            None,
            None,
            "SEGMENT_2",
            "Y",
            "Y",
            "N",
            "0.0",
            "210",
            "450",
            "0510706679",
            "35446510",
            "SAMSUNG KOREA",
            "GALAXY A50",
            "SMARTPHONE",
            "Y",
            "ANDROID",
            "4G",
            "Y",
            "N",
            None,
            None,
            None,
            None,
        ],
        [
            None,
            "2020-01-27",
            "D",
            "202001",
            None,
            None,
            None,
            None,
            None,
            "SEGMENT_2",
            "Y",
            "Y",
            "Y",
            "2.7457729E7",
            None,
            None,
            "0309048041",
            "35512309",
            "SAMSUNG KOREA",
            "GALAXY A8+ SM-A730F/DS",
            "SMARTPHONE",
            "Y",
            "ANDROID",
            "4G",
            "N",
            "N",
            None,
            None,
            None,
            None,
        ],
        [
            "212-452",
            "2020-01-27",
            "C",
            "202001",
            None,
            None,
            None,
            None,
            None,
            "SEGMENT_2",
            "Y",
            "Y",
            "Y",
            "9050384.0",
            "212",
            "452",
            "52107270262",
            "352452107",
            "SAMSUNG KOREA",
            "SAMSUNG SM-J500G/DS",
            "SMARTPHONE",
            "Y",
            "ANDROID",
            "4G",
            "Y",
            "Y",
            None,
            None,
            None,
            None,
        ],
        [
            "214-454",
            "2020-01-27",
            "F",
            "202001",
            None,
            None,
            None,
            None,
            None,
            "SEGMENT_2",
            "N",
            "Y",
            "N",
            "0.0",
            "214",
            "454",
            "0003781937",
            "86463003",
            "ONEPLUS TECHNOLOGY (SHENZHEN) CO LTD",
            "ONEPLUS 5",
            "SMARTPHONE",
            "Y",
            "ANDROID",
            "4G",
            "N",
            "Y",
            None,
            None,
            None,
            None,
        ],
        [
            "213-453",
            "2020-01-27",
            "E",
            "202001",
            "Samsung SM-G950U",
            "Smartphone",
            ["11090538958"],
            None,
            None,
            "SEGMENT_2",
            "Y",
            "Y",
            "N",
            "0.0",
            "213",
            "453",
            "0207096301",
            "35720207",
            "SAMSUNG KOREA",
            "SAMSUNG SM-J510FN/DS",
            "SMARTPHONE",
            "Y",
            "ANDROID",
            "4G",
            "Y",
            "N",
            None,
            None,
            None,
            None,
        ],
    ]
    #     assert expected_join_nopartner_final == [
    #         [i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], i[10], i[11], i[12], i[13], i[14], i[15], i[16],
    #          i[17], i[18], i[19], i[20], i[21], i[22], i[23], i[24], i[25], i[26], i[27], i[28], i[29]] for i in
    #         out_join_nopartner_final.collect()]

    # downsampling
    out_downsampled_partner = downsampling_master_table(
        out_join_partner_final, fraction=0.00001
    )
    expected_downsampled_partner = []
    assert sorted(expected_downsampled_partner) == sorted(
        [
            [
                i[0],
                i[1],
                i[2],
                i[3],
                i[4],
                i[5],
                i[6],
                i[7],
                i[8],
                i[9],
                i[10],
                i[11],
                i[12],
                i[13],
                i[14],
                i[15],
                i[16],
                i[17],
                i[18],
                i[19],
                i[20],
                i[21],
                i[22],
                i[23],
                i[24],
                i[25],
                i[26],
                i[27],
                i[28],
            ]
            for i in out_downsampled_partner.collect()
        ]
    )

    out_downsampled_nopartner = downsampling_master_table(
        out_join_nopartner_final, fraction=0.00001
    )
    expected_downsampled_nopartner = []
    assert sorted(expected_downsampled_nopartner) == sorted(
        [
            [
                i[0],
                i[1],
                i[2],
                i[3],
                i[4],
                i[5],
                i[6],
                i[7],
                i[8],
                i[9],
                i[10],
                i[11],
                i[12],
                i[13],
                i[14],
                i[15],
                i[16],
                i[17],
                i[18],
                i[19],
                i[20],
                i[21],
                i[22],
                i[23],
                i[24],
                i[25],
                i[26],
                i[27],
                i[28],
                i[29],
            ]
            for i in out_downsampled_nopartner.collect()
        ]
    )


# --------------------------------unit testing data cleansing--------------------------#
# --------------------unit testing select df to score or train and pick_a_segment_and_month--------------------#
def test_select_df_to_score_or_train_test(spark_session):
    rech_df_schema = StructType(
        [
            StructField("msisdn", StringType(), True),
            StructField("weekstart", StringType(), True),
            StructField("fea_handset_count_01m", IntegerType(), True),
            StructField("fea_handset_count_02m", IntegerType(), True),
            StructField("fea_handset_count_03m", IntegerType(), True),
            StructField("fea_handset_count_05m", IntegerType(), True),
            StructField("segment", StringType(), True),
        ]
    )
    # train
    train_df = spark_session.createDataFrame(
        [
            ("H", "2020-01-27", 1, 1, 1, 1, "SEGMENT_2"),
            ("I", "2020-01-27", 1, 1, 1, 2, "SEGMENT_2"),
            ("J", "2020-01-20", 4, 6, 6, 6, "SEGMENT_2"),
            ("K", "2020-01-27", 1, 1, 1, 1, "SEGMENT_5"),
            ("L", "2020-01-27", 1, 1, 1, 1, "SEGMENT_5"),
        ],
        rech_df_schema,
    )
    # score
    score_df = spark_session.createDataFrame(
        [
            ("H", "2020-01-27", 1, 1, 1, 1, "SEGMENT_2"),
            ("I", "2020-01-27", 1, 1, 1, 2, "SEGMENT_2"),
            ("J", "2020-01-27", 4, 6, 6, 6, "SEGMENT_2"),
            ("K", "2020-01-27", 1, 1, 1, 1, "SEGMENT_2"),
            ("L", "2020-01-20", 1, 1, 1, 1, "SEGMENT_2"),
            ("M", "2020-01-27", 1, 1, 1, 1, "SEGMENT_5"),
            ("N", "2020-01-27", 1, 1, 1, 1, "SEGMENT_5"),
        ],
        rech_df_schema,
    )
    # select table
    out_train = select_df_to_score_or_train(train_df, train_df, "train")
    out_score = select_df_to_score_or_train(score_df, score_df, "score")
    expected_out_train = 5
    expected_out_score = 7
    # pick segment and month
    out_segment_train = pick_a_segment_and_month(out_train, "SEGMENT_2", "2020-01-27")
    out_segment_score = pick_a_segment_and_month(out_score, "SEGMENT_2", "2020-01-27")
    expected_segment_train = 2
    expected_segment_score = 4

    assert out_train.count() == expected_out_train
    assert out_score.count() == expected_out_score
    assert out_segment_train.count() == expected_segment_train
    assert out_segment_score.count() == expected_segment_score


# --------------------unit testing cast_decimals_to_float--------------------#
def test_cast_decimals_to_float_test(spark_session):
    rech_df_schema = StructType(
        [
            StructField("lac_ci", ArrayType(StringType(), True), True),
            StructField("weekstart", StringType(), True),
            StructField("month", StringType(), True),
            StructField("fea_rech_tot_amt_avg_02w", DecimalType(38, 6), True),
            StructField("fea_rech_tot_amt_avg_03w", DecimalType(38, 6), True),
            StructField("fea_rech_tot_amt_avg_01m", DecimalType(38, 6), True),
            StructField("fea_rech_tot_amt_avg_02m", DecimalType(38, 6), True),
            StructField("fea_rech_tot_amt_avg_03m", DecimalType(38, 6), True),
        ]
    )
    # dataframe,
    df = spark_session.createDataFrame(
        [
            (None, "2020-01-27", "202001", None, None, None, None, None),
            (None, "2020-01-27", "202001", None, None, None, None, None),
            (
                None,
                "2020-01-27",
                "202001",
                Decimal("15000.000000"),
                Decimal("15000.000000"),
                Decimal("15000.000000"),
                Decimal("15000.000000"),
                Decimal("15000.000000"),
            ),
            (None, "2020-01-27", "202001", None, None, None, None, None),
            (None, "2020-01-27", "202001", None, None, None, None, None),
            (None, "2020-01-27", "202001", None, None, None, None, None),
            (None, "2020-01-27", "202001", None, None, None, None, None),
        ],
        rech_df_schema,
    )
    # cast decimals to float
    out = cast_decimals_to_float(df)
    out = [col for col, ty in out.toPandas().dtypes.items() if ty == "float32"]
    expected_out = [
        "fea_rech_tot_amt_avg_02w",
        "fea_rech_tot_amt_avg_03w",
        "fea_rech_tot_amt_avg_01m",
        "fea_rech_tot_amt_avg_02m",
        "fea_rech_tot_amt_avg_03m",
    ]
    assert sorted(out) == sorted(expected_out)


# --------------unit testing subset_features, specific_transformations, to_pandas,remove unit analysist, transforming_columns-------------#
def test_subset_features_test(spark_session):
    rech_df_schema = StructType(
        [
            StructField("manufacture", StringType(), True),
            StructField("fea_handset_imeis", ArrayType(StringType(), True), True),
            StructField("lac_ci", StringType(), True),
            StructField("weekstart", StringType(), True),
            StructField("msisdn", StringType(), True),
            StructField("fea_custprof_segment_data_user", StringType(), True),
            StructField("fea_custprof_status", StringType(), True),
            StructField("fea_custprof_brand", StringType(), True),
            StructField("fea_custprof_arpu_segment_name", StringType(), True),
        ]
    )
    # dataframe,
    df = spark_session.createDataFrame(
        [
            (
                "QUECTEL WIRELESS SOLUTIONS CO LTD",
                ["86893704529929000", "868937045299290"],
                "10005-25804",
                "2020-01-27",
                "111",
                "N",
                "A",
                "Loop",
                "VERY HIGH",
            ),
            (
                "XIAOMI COMMUNICATIONS CO LTD",
                ["35706107150565"],
                "10005-25804",
                "2020-01-27",
                "222",
                "N",
                "A",
                "kartuAS",
                "MEDIUM",
            ),
            (
                "QUECTEL WIRELESS SOLUTIONS CO LTD",
                ["35975508386859"],
                "10005-25804",
                "2020-01-27",
                "333",
                "N",
                "A",
                "simPATI",
                "VERY LOW",
            ),
            (
                "XIAOMI COMMUNICATIONS CO LTD",
                ["86850203537080"],
                "10005-25804",
                "2020-01-27",
                "444",
                "N",
                "A",
                "simPATI",
                "VERY LOW",
            ),
            (
                "APPLE INC",
                ["35829810010919"],
                "10062-38429",
                "2020-01-27",
                "555",
                "N",
                "A",
                "simPATI",
                "VERY LOW",
            ),
            (
                "GUANGDONG OPPO MOBILE TELECOMMUNICATIONS CORP LTD",
                ["869723037374452"],
                "10062-38429",
                "2020-01-27",
                "666",
                "N",
                "A",
                "simPATI",
                "VERY LOW",
            ),
            (
                "XIAOMI COMMUNICATIONS CO LTD",
                ["86888903713027", "8688890371302705"],
                "10062-38429",
                "2020-01-27",
                "777",
                "N",
                "A",
                "Loop",
                "VERY HIGH",
            ),
        ],
        rech_df_schema,
    )
    # select table columns
    features_to_select = [
        "fea_handset_imeis",
        "lac_ci",
        "weekstart",
        "msisdn",
        "fea_custprof_segment_data_user",
        "fea_custprof_status",
        "fea_custprof_brand",
    ]
    out = subset_features(df, features_to_select)
    expected_out = 7
    assert out.count() == expected_out
    # specific transformation
    out_specific = specific_transformations(out)
    expected_out_specific = 7
    assert out_specific.count() == expected_out_specific
    # topandas
    out_pandas = to_pandas(out_specific)
    expected_out_pandas = [
        2,
        "10005-25804",
        "2020-01-27",
        "111",
        "N",
        "A",
        "Loop",
    ]
    assert out_pandas.to_numpy()[0].tolist() == expected_out_pandas
    # remove unit analysist
    out_remove = remove_unit_of_analysis(out_pandas, ["lac_ci", "weekstart", "msisdn"])
    out_remove_cols = [[i[0], i[1], i[2], i[3]] for i in out_remove.to_numpy().tolist()]
    expected_out_remove = [
        [2, "N", "A", "Loop"],
        [1, "N", "A", "kartuAS"],
        [1, "N", "A", "simPATI"],
        [1, "N", "A", "simPATI"],
        [1, "N", "A", "simPATI"],
        [1, "N", "A", "simPATI"],
        [2, "N", "A", "Loop"],
    ]
    assert out_remove_cols == expected_out_remove
    # transforming_columns
    (
        out_transform_to_log,
        out_numerical_columns,
        out_categorical_columns,
    ) = transforming_columns(out_remove, ["fea_handset_imeis"], "zero", 2)
    expected_out_transfor_to_log = [
        [1.0986122886681098, "N", "A", "Loop"],
        [0.69452471805599453, "N", "A", "kartuAS"],
        [0.69452471805599453, "N", "A", "simPATI"],
        [0.69452471805599453, "N", "A", "simPATI"],
        [0.69452471805599453, "N", "A", "simPATI"],
        [0.69452471805599453, "N", "A", "simPATI"],
        [1.0986122886681098, "N", "A", "Loop"],
    ]
    expected_out_numerical_columns = ["fea_handset_imeis"]
    expected_out_categorical_columns = [
        "fea_custprof_brand",
        "fea_custprof_segment_data_user",
        "fea_custprof_status",
    ]
    # assert sorted(expected_out_transfor_to_log) == sorted([[i[0], i[1], i[2], i[3]] for i in out_transform_to_log.to_numpy().tolist()])
    assert sorted(expected_out_numerical_columns) == sorted(out_numerical_columns)
    assert sorted(expected_out_categorical_columns) == sorted(out_categorical_columns)


# -------------------------------unit testing dimension reduction-------------------------------#
# --------------------unit testing numerical categorical--------------------#
# parameterize
dimension_reduction_4G_parameter = {
    "n_components_reduction": 2,
    "n_iter_reduction": 3,
    "copy_reduction": True,
    "check_input_reduction": True,
    "engine_reduction": "auto",
    "random_state_reduction": 42,
}
# Create DataFrame
data = {
    "fea_custprof_los": [-0.370340, -0.358043, -0.286849, -0.291380, -295910],
    "fea_custprof_segment_hvc_mtd": [
        -0.167416,
        5.973094,
        -0.167416,
        -0.167416,
        5.973094,
    ],
    "fea_custprof_nik_age": [0.884220, -0.486194, 0.778804, -1.065985, 0.884220],
    "fea_custprof_arpu_segment_name": [
        "VERY LOW",
        "VERY HIGH",
        "HIGH",
        "VERY LOW",
        "VERY LOW",
    ],
    "volte": ["N", "N", "Y", "Y", "Y"],
    "fea_custprof_brand": ["Loop", "Loop", "simPATI", "kartuAS", "simPATI"],
    "multisim": ["Y", "N", "Y", "Y", "N"],
}
df_norm_segment = pd.DataFrame(data)  # --df all
numerical_columns = df_norm_segment.select_dtypes(
    np.number
).columns.tolist()  # --list numerical columns
categorical_columns = list(
    set(df_norm_segment.columns) - set(numerical_columns)
)  # --list categorical columns
df_numerical = df_norm_segment.loc[:, numerical_columns]  # df numerical
df_categorical = df_norm_segment.loc[:, categorical_columns]  # df categorical


def test_numerical_categorical_test():
    output_numerical, output_categorical, num_cols, cat_cols = numerical_categorical(
        df_norm_segment
    )

    expected_num_cols = [
        "fea_custprof_los",
        "fea_custprof_segment_hvc_mtd",
        "fea_custprof_nik_age",
    ]
    expected_cat_cols = [
        "fea_custprof_arpu_segment_name",
        "volte",
        "multisim",
        "fea_custprof_brand",
    ]
    expected_num_value_df = [-0.37034, -0.167416, 0.88422]
    expected_cat_value_df = ["VERY LOW", "N", "Y", "Loop"]

    assert sorted(expected_num_cols) == sorted(num_cols)
    assert sorted(expected_cat_cols) == sorted(cat_cols)
    assert sorted(expected_num_value_df) == sorted(output_numerical.loc[0].tolist())
    assert sorted(expected_cat_value_df) == sorted(output_categorical.loc[0].tolist())


# --------------------unit testing apply PCA and transform dimension reduction--------------------#
# PCA
pca = PCA(
    n_components=dimension_reduction_4G_parameter["n_components_reduction"],
    n_iter=dimension_reduction_4G_parameter["n_iter_reduction"],
    copy=dimension_reduction_4G_parameter["copy_reduction"],
    check_input=dimension_reduction_4G_parameter["check_input_reduction"],
    engine=dimension_reduction_4G_parameter["engine_reduction"],
    random_state=dimension_reduction_4G_parameter["random_state_reduction"],
)


def test_apply_PCA_test():
    # PCA
    pca = PCA(
        n_components=dimension_reduction_4G_parameter["n_components_reduction"],
        n_iter=dimension_reduction_4G_parameter["n_iter_reduction"],
        copy=dimension_reduction_4G_parameter["copy_reduction"],
        check_input=dimension_reduction_4G_parameter["check_input_reduction"],
        engine=dimension_reduction_4G_parameter["engine_reduction"],
        random_state=dimension_reduction_4G_parameter["random_state_reduction"],
    )

    pcas, pcas_correlations, pcas_explained_inertia, scaler = apply_PCA(
        df_numerical, dimension_reduction_4G_parameter
    )

    expected_pcas = 2
    expected_pcas_correlations = [-0.933, 0.77, 0.527]
    expected_pcas_explained_inertia = [0.581, 0.333]
    expected_scaler_sample = 5

    assert expected_pcas == pcas.n_components
    assert sorted(expected_pcas_correlations) == sorted(
        [round(i, 3) for i in pcas_correlations[0].tolist()]
    )
    assert sorted(expected_pcas_explained_inertia) == sorted(
        [round(i, 3) for i in pcas_explained_inertia]
    )
    assert expected_scaler_sample == scaler.n_samples_seen_

    # transform PCA, apply clusters
    x_pca = transform_dimension_reduction(
        df_norm_segment, numerical_columns, categorical_columns, pcas, scaler
    )
    expected_x_pca = [-0.496, 0.027, -0.547, -1.448, 2.463]

    assert sorted(expected_x_pca) == sorted([round(i, 3) for i in x_pca[0].tolist()])


# --------------------kmeans_clustering_fit--------------------#
clustering_4G_parameter = {
    "n_clusters": 2,
    "n_init_clusters": 4,
    "max_iter_clusters": 1500,
    "n_jobs_clusters": -1,
    "random_state_clustering": 42,
}
x_pca = pd.DataFrame(
    np.array(
        [
            [-4.95642612e-01, -1.15243859e00],
            [2.68030209e-02, 1.382926452e00],
            [-5.47095809e-01, -1.04606011e00],
            [-1.44752239e00, 8.15572416e-01],
            [2.46345779e00, -2.00505874e-08],
        ]
    ),
    columns=[0, 1],
)


def test_apply_kmean_clustering_fit_test():
    kmeans_fit = kmeans_clustering_fit(x_pca, clustering_4G_parameter)
    expected_kmeans_cluster_centers = [-0.616, 0.0]
    assert expected_kmeans_cluster_centers == [
        round(i, 3) for i in kmeans_fit.cluster_centers_[0].tolist()
    ]

    kmeans_fit = kmeans_clustering_fit(x_pca, clustering_4G_parameter, max_rows=3)
    expected_kmeans_cluster_centers = [2.463, 0.0]
    assert expected_kmeans_cluster_centers == [
        round(i, 3) for i in kmeans_fit.cluster_centers_[0].tolist()
    ]


def test_apply_kmean_clustering_predict_test():
    km = KMeans(
        algorithm="auto",
        copy_x=True,
        init="k-means++",
        max_iter=1500,
        n_clusters=2,
        n_init=4,
        n_jobs=-1,
        precompute_distances="auto",
        random_state=42,
        tol=0.0001,
        verbose=0,
    )

    km.fit(x_pca)
    x_pca_ = pd.DataFrame(
        np.array([[-4.95642612e-01, -1.15243859e00], [0, 3], [0, 452], [10, -32]]),
        columns=[0, 1],
    )
    kmeans_pred_pca = kmeans_clustering_predict(x_pca_, km)
    expected_segment_clustering = [0, 0, 0, 1]
    assert np.all(expected_segment_clustering == kmeans_pred_pca["segment_clustering"])


def test_joining_kmeans_result_and_original_table():
    df1 = pd.DataFrame([[1, 2], [3, 4]])
    df2 = pd.DataFrame([[5, 6], [7, 8]], columns=["a", "b"])
    expected = pd.DataFrame([[1, 2, 5, 6], [3, 4, 7, 8]], columns=["0", "1", "a", "b"])
    out = joining_kmeans_result_and_original_table(df1, df2)
    assert all(out == expected)


def test_joining_with_complete_master_table(spark_session):
    df1 = spark_session.createDataFrame(
        pd.DataFrame([[1, 2], [3, 4]], columns=["msisdn", "segment_clustering"])
    )
    df2 = spark_session.createDataFrame(
        pd.DataFrame([[1, 33], [3, 6]], columns=["msisdn", "a"])
    )
    res = joining_with_complete_master_table(df1, df2)
    assert res.columns == ["msisdn", "a", "segment_clustering"]
    assert np.all(np.array(res.collect()) == [[1, 33, 2], [3, 6, 4]])


def test_specific_transformations(spark_session):
    df_pandas = pd.DataFrame(np.arange(5).reshape(-1, 1), columns=["msisdn"])
    df_pandas["fea_handset_imeis"] = [[1], [2, 2], [3, 3, 3], [], None]
    df_spark = spark_session.createDataFrame(df_pandas)

    out_pandas = specific_transformations(df_pandas)
    out_spark = specific_transformations(df_spark)

    assert np.all(out_pandas.columns == out_spark.columns)
    assert np.all(out_pandas.columns == ["msisdn", "fea_handset_imeis"])

    assert np.all(np.array(out_spark.collect()) == out_pandas.values)
    assert np.all(out_pandas.values == [[0, 1], [1, 2], [2, 3], [3, 0], [4, 0]])


def test_joining_prepaid_postpaid_features_test(spark_session):
    rech_df_schema = StructType(
        [
            StructField("msisdn", StringType(), True),
            StructField("weekstart", StringType(), True),
            StructField("fea_handset_count_01m", IntegerType(), True),
            StructField("fea_handset_count_02m", IntegerType(), True),
            StructField("fea_handset_count_03m", IntegerType(), True),
            StructField("fea_handset_count_05m", IntegerType(), True),
        ]
    )
    rech_df = spark_session.createDataFrame(
        [
            ("H", "2020-01-27", 1, 1, 1, 1),
            ("I", "2020-01-27", 1, 1, 1, 2),
            ("J", "2020-01-27", 4, 6, 6, 6),
            ("K", "2020-01-27", 1, 1, 1, 1),
            ("L", "2020-01-27", 1, 1, 1, 1),
            ("M", "2020-01-27", 1, 1, 1, 1),
            ("N", "2020-01-27", 1, 1, 1, 1),
        ],
        rech_df_schema,
    )
    expected_out = [
        ["L", "2020-01-27", 1, 1, 1, 1],
        ["K", "2020-01-27", 1, 1, 1, 1],
        ["I", "2020-01-27", 1, 1, 1, 2],
        ["J", "2020-01-27", 4, 6, 6, 6],
        ["M", "2020-01-27", 1, 1, 1, 1],
        ["N", "2020-01-27", 1, 1, 1, 1],
        ["H", "2020-01-27", 1, 1, 1, 1],
    ]
    out = joining_prepaid_postpaid_features(rech_df)
    out = [[i[0], i[1], i[2], i[3], i[4], i[5]] for i in out.collect()]
    assert sorted(out) == sorted(expected_out)


# --------------------unit testing downsampled, to_pandas, and run_statistics_profile--------------------#
def test_run_statistics_profile_test(spark_session):
    rech_df_schema = StructType(
        [
            StructField("msisdn", StringType(), True),
            StructField("weekstart", StringType(), True),
            StructField("fea_handset_count_01m", IntegerType(), True),
            StructField("fea_handset_count_02m", IntegerType(), True),
            StructField("fea_handset_count_03m", IntegerType(), True),
            StructField("fea_handset_count_05m", IntegerType(), True),
            StructField("segment_clustering", IntegerType(), True),
        ]
    )
    rech_df = spark_session.createDataFrame(
        [
            ("H", "2020-01-27", 1, 1, 1, 1, 1),
            ("I", "2020-01-27", 1, 1, 1, 2, 5),
            ("J", "2020-01-27", 4, 6, 6, 6, 6),
            ("K", "2020-01-27", 1, 1, 1, 1, 2),
            ("L", "2020-01-27", 1, 1, 1, 1, 2),
            ("M", "2020-01-27", 1, 1, 1, 1, 4),
            ("N", "2020-01-27", 1, 1, 1, 1, 5),
        ],
        rech_df_schema,
    )
    # sample size
    sample_size_to_compute_stats = 6
    expected_out = 18
    out = downsample(rech_df, sample_size_to_compute_stats)
    out_down = pd.DataFrame(out.collect(), columns=out.columns)
    out_run = run_statistics_profile(out_down)
    out_run = out_run.to_numpy().tolist()
    assert len(out_run[2]) == expected_out


def test_conncat_results(spark_session):
    schema = StructType(
        [StructField("a", StringType(), True), StructField("b", IntegerType(), True)]
    )
    res = concatenate_results_of_clustering(
        spark_session.createDataFrame([("v1", 1),], schema),
        spark_session.createDataFrame([("v2", 2),], schema),
        spark_session.createDataFrame([("v3", 3),], schema),
    )

    assert np.array(res.collect()).tolist() == [["v1", "1"], ["v2", "2"], ["v3", "3"]]
