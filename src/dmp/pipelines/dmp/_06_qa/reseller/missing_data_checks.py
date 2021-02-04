import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, StringType, StructField, StructType

from utils import next_week_start_day


def check_join_accuracies(
    df_outlet_id_msisdn_mapping, dfj_digipos_mkios, df_dnkm, df_urp
):
    schema = StructType(
        [
            StructField("left_table", StringType(), True),
            StructField("right_table", StringType(), True),
            StructField("join_condition", StringType(), True),
            StructField("perc_join_accuracy", FloatType(), True),
        ]
    )

    data = []

    tot_rs_msisdn = (
        df_outlet_id_msisdn_mapping.select("outlet_msisdn").distinct().count()
    )
    rs_msisdns_matched = dfj_digipos_mkios.select("rs_msisdn").distinct().count()

    left_table_path = "l1_outlet_id_msisdn_mapping"
    right_table_path = "l1_dfj_digipos_mkios"
    join_on = "left_table.outlet_msisdn == right_table.rs_msisdn"
    perc_join_accuracy = round(rs_msisdns_matched / tot_rs_msisdn, 2)
    data_point = (
        left_table_path,
        right_table_path,
        join_on,
        float(perc_join_accuracy),
    )
    data.append(data_point)

    df_dnkm = df_dnkm.filter(
        (f.col("lac_ci").isNotNull()) & (f.col("lac_ci") != "")
    ).withColumn("lac_ci", f.regexp_replace("lac_ci", "-", "|"))
    df_urp = df_urp.filter(
        (f.col("bnum_lacci_id").isNotNull()) & (f.col("bnum_lacci_id") != "")
    )

    dfj = (
        df_dnkm.select("lac_ci")
        .distinct()
        .join(
            df_urp.select("bnum_lacci_id").distinct(),
            f.col("lac_ci") == f.col("bnum_lacci_id"),
        )
    )

    tot_lacci = df_urp.select("bnum_lacci_id").distinct().count()
    matched_lacci = dfj.count()

    left_table_path = "l1_dnkm"
    right_table_path = "l1_rech_urp_dd"
    join_on = "left_table.lac_ci == right_table.bnum_lacci_id"
    perc_join_accuracy = round(matched_lacci / tot_lacci, 2)
    data_point = (
        left_table_path,
        right_table_path,
        join_on,
        float(perc_join_accuracy),
    )
    data.append(data_point)

    spark = SparkSession.builder.getOrCreate()
    df_join_acc_check = spark.createDataFrame(data, schema=schema)
    df_join_acc_check = df_join_acc_check.withColumn(
        "run_date_time", f.current_timestamp()
    )
    df_join_acc_check = df_join_acc_check.withColumn(
        "weekstart", next_week_start_day("run_date_time")
    )
    return df_join_acc_check
