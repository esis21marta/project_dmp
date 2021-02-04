training_catalog = {
    "aggregation_catalog_name_a_1_qa_1w": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "filepath": "dummy/file/path",
        "save_args": {"mode": "overwrite", "partitionBy": "weekstart"},
        "load_args": {
            "partition_filter_period": "1w",
            "partition_column": "weekstart",
            "partition_date_format": "%Y-%m-%d",
        },
    },
    "aggregation_catalog_name_a_2_qa_1w": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "filepath": "dummy/file/path",
        "save_args": {"mode": "overwrite", "partitionBy": "weekstart"},
        "load_args": {
            "partition_filter_period": "1w",
            "partition_column": "weekstart",
            "partition_date_format": "%Y-%m-%d",
        },
    },
    "aggregation_catalog_name_b_1_qa_1w": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "filepath": "dummy/file/path",
        "save_args": {"mode": "overwrite", "partitionBy": "weekstart"},
        "load_args": {
            "partition_filter_period": "1w",
            "partition_column": "weekstart",
            "partition_date_format": "%Y-%m-%d",
        },
    },
    "l4_qa_aggregation_aggregation_table_name_a_1_metrics": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_training/06_qa_check/aggregation/aggregation_table_name_a_1_aggregation/metrics.parquet",
    },
    "l4_qa_aggregation_aggregation_table_name_a_1_monthly_unique_msisdn": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_training/06_qa_check/aggregation/aggregation_table_name_a_1_aggregation/monthly_unique_msisdn.parquet",
    },
    "l4_qa_aggregation_aggregation_table_name_a_1_outliers": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_training/06_qa_check/aggregation/aggregation_table_name_a_1_aggregation/outliers.parquet",
    },
    "l4_qa_aggregation_aggregation_table_name_a_2_metrics": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_training/06_qa_check/aggregation/aggregation_table_name_a_2_aggregation/metrics.parquet",
    },
    "l4_qa_aggregation_aggregation_table_name_a_2_monthly_unique_msisdn": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_training/06_qa_check/aggregation/aggregation_table_name_a_2_aggregation/monthly_unique_msisdn.parquet",
    },
    "l4_qa_aggregation_aggregation_table_name_a_2_outliers": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_training/06_qa_check/aggregation/aggregation_table_name_a_2_aggregation/outliers.parquet",
    },
    "l4_qa_aggregation_aggregation_table_name_b_1_metrics": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_training/06_qa_check/aggregation/aggregation_table_name_b_1_aggregation/metrics.parquet",
    },
    "l4_qa_aggregation_aggregation_table_name_b_1_monthly_unique_msisdn": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_training/06_qa_check/aggregation/aggregation_table_name_b_1_aggregation/monthly_unique_msisdn.parquet",
    },
    "l4_qa_aggregation_aggregation_table_name_b_1_outliers": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_training/06_qa_check/aggregation/aggregation_table_name_b_1_aggregation/outliers.parquet",
    },
    "l4_qa_feature_feature_table_name_a_1_metrics": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_training/06_qa_check/feature/feature_table_name_a_1_feature/metrics.parquet",
    },
    "l4_qa_feature_feature_table_name_a_1_monthly_unique_msisdn": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_training/06_qa_check/feature/feature_table_name_a_1_feature/monthly_unique_msisdn.parquet",
    },
    "l4_qa_feature_feature_table_name_a_1_outliers": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_training/06_qa_check/feature/feature_table_name_a_1_feature/outliers.parquet",
    },
    "l4_qa_feature_feature_table_name_a_2_metrics": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_training/06_qa_check/feature/feature_table_name_a_2_feature/metrics.parquet",
    },
    "l4_qa_feature_feature_table_name_a_2_monthly_unique_msisdn": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_training/06_qa_check/feature/feature_table_name_a_2_feature/monthly_unique_msisdn.parquet",
    },
    "l4_qa_feature_feature_table_name_a_2_outliers": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_training/06_qa_check/feature/feature_table_name_a_2_feature/outliers.parquet",
    },
    "l4_qa_feature_feature_table_name_b_1_metrics": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_training/06_qa_check/feature/feature_table_name_b_1_feature/metrics.parquet",
    },
    "l4_qa_feature_feature_table_name_b_1_monthly_unique_msisdn": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_training/06_qa_check/feature/feature_table_name_b_1_feature/monthly_unique_msisdn.parquet",
    },
    "l4_qa_feature_feature_table_name_b_1_outliers": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_training/06_qa_check/feature/feature_table_name_b_1_feature/outliers.parquet",
    },
    "l4_qa_aggregation_metrics": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_training/06_qa_check/aggregation/metrics.parquet",
    },
    "l4_qa_aggregation_monthly_unique_msisdn": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_training/06_qa_check/aggregation/monthly_unique_msisdn.parquet",
    },
    "l4_qa_aggregation_outliers": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_training/06_qa_check/aggregation/outliers.parquet",
    },
    "l4_qa_feature_metrics": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_training/06_qa_check/feature/metrics.parquet",
    },
    "l4_qa_feature_monthly_unique_msisdn": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_training/06_qa_check/feature/monthly_unique_msisdn.parquet",
    },
    "l4_qa_feature_outliers": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_training/06_qa_check/feature/outliers.parquet",
    },
    "l4_qa_master_metrics": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_training/06_qa_check/master/metrics.parquet",
    },
    "l4_qa_master_monthly_unique_msisdn": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_training/06_qa_check/master/monthly_unique_msisdn.parquet",
    },
    "l4_qa_master_outliers": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_training/06_qa_check/master/outliers.parquet",
    },
}


scoring_catalog = {
    "aggregation_catalog_name_a_1_qa_1w": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "filepath": "dummy/file/path",
        "save_args": {"mode": "overwrite", "partitionBy": "weekstart"},
        "load_args": {
            "partition_filter_period": "1w",
            "partition_column": "weekstart",
            "partition_date_format": "%Y-%m-%d",
        },
    },
    "aggregation_catalog_name_a_2_qa_1w": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "filepath": "dummy/file/path",
        "save_args": {"mode": "overwrite", "partitionBy": "weekstart"},
        "load_args": {
            "partition_filter_period": "1w",
            "partition_column": "weekstart",
            "partition_date_format": "%Y-%m-%d",
        },
    },
    "aggregation_catalog_name_b_1_qa_1w": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "filepath": "dummy/file/path",
        "save_args": {"mode": "overwrite", "partitionBy": "weekstart"},
        "load_args": {
            "partition_filter_period": "1w",
            "partition_column": "weekstart",
            "partition_date_format": "%Y-%m-%d",
        },
    },
    "l4_qa_aggregation_aggregation_table_name_a_1_metrics": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_score/06_qa_check/aggregation/aggregation_table_name_a_1_aggregation/metrics.parquet",
    },
    "l4_qa_aggregation_aggregation_table_name_a_1_monthly_unique_msisdn": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_score/06_qa_check/aggregation/aggregation_table_name_a_1_aggregation/monthly_unique_msisdn.parquet",
    },
    "l4_qa_aggregation_aggregation_table_name_a_1_outliers": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_score/06_qa_check/aggregation/aggregation_table_name_a_1_aggregation/outliers.parquet",
    },
    "l4_qa_aggregation_aggregation_table_name_a_2_metrics": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_score/06_qa_check/aggregation/aggregation_table_name_a_2_aggregation/metrics.parquet",
    },
    "l4_qa_aggregation_aggregation_table_name_a_2_monthly_unique_msisdn": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_score/06_qa_check/aggregation/aggregation_table_name_a_2_aggregation/monthly_unique_msisdn.parquet",
    },
    "l4_qa_aggregation_aggregation_table_name_a_2_outliers": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_score/06_qa_check/aggregation/aggregation_table_name_a_2_aggregation/outliers.parquet",
    },
    "l4_qa_aggregation_aggregation_table_name_b_1_metrics": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_score/06_qa_check/aggregation/aggregation_table_name_b_1_aggregation/metrics.parquet",
    },
    "l4_qa_aggregation_aggregation_table_name_b_1_monthly_unique_msisdn": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_score/06_qa_check/aggregation/aggregation_table_name_b_1_aggregation/monthly_unique_msisdn.parquet",
    },
    "l4_qa_aggregation_aggregation_table_name_b_1_outliers": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_score/06_qa_check/aggregation/aggregation_table_name_b_1_aggregation/outliers.parquet",
    },
    "l4_qa_feature_feature_table_name_a_1_metrics": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_score/06_qa_check/feature/feature_table_name_a_1_feature/metrics.parquet",
    },
    "l4_qa_feature_feature_table_name_a_1_monthly_unique_msisdn": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_score/06_qa_check/feature/feature_table_name_a_1_feature/monthly_unique_msisdn.parquet",
    },
    "l4_qa_feature_feature_table_name_a_1_outliers": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_score/06_qa_check/feature/feature_table_name_a_1_feature/outliers.parquet",
    },
    "l4_qa_feature_feature_table_name_a_2_metrics": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_score/06_qa_check/feature/feature_table_name_a_2_feature/metrics.parquet",
    },
    "l4_qa_feature_feature_table_name_a_2_monthly_unique_msisdn": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_score/06_qa_check/feature/feature_table_name_a_2_feature/monthly_unique_msisdn.parquet",
    },
    "l4_qa_feature_feature_table_name_a_2_outliers": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_score/06_qa_check/feature/feature_table_name_a_2_feature/outliers.parquet",
    },
    "l4_qa_feature_feature_table_name_b_1_metrics": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_score/06_qa_check/feature/feature_table_name_b_1_feature/metrics.parquet",
    },
    "l4_qa_feature_feature_table_name_b_1_monthly_unique_msisdn": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_score/06_qa_check/feature/feature_table_name_b_1_feature/monthly_unique_msisdn.parquet",
    },
    "l4_qa_feature_feature_table_name_b_1_outliers": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_score/06_qa_check/feature/feature_table_name_b_1_feature/outliers.parquet",
    },
    "l4_qa_aggregation_metrics": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_score/06_qa_check/aggregation/metrics.parquet",
    },
    "l4_qa_aggregation_monthly_unique_msisdn": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_score/06_qa_check/aggregation/monthly_unique_msisdn.parquet",
    },
    "l4_qa_aggregation_outliers": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_score/06_qa_check/aggregation/outliers.parquet",
    },
    "l4_qa_feature_metrics": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_score/06_qa_check/feature/metrics.parquet",
    },
    "l4_qa_feature_monthly_unique_msisdn": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_score/06_qa_check/feature/monthly_unique_msisdn.parquet",
    },
    "l4_qa_feature_outliers": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_score/06_qa_check/feature/outliers.parquet",
    },
    "l4_qa_master_metrics": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_score/06_qa_check/master/metrics.parquet",
    },
    "l4_qa_master_monthly_unique_msisdn": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_score/06_qa_check/master/monthly_unique_msisdn.parquet",
    },
    "l4_qa_master_outliers": {
        "type": "src.dmp.io.spark_data_set.SparkDataSet",
        "file_format": "parquet",
        "save_args": {"mode": "overwrite"},
        "filepath": "mck_dmp_score/06_qa_check/master/outliers.parquet",
    },
}