import pyspark
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType


def timeliness_score(
    df_timeliness: pyspark.sql.DataFrame,
    df_master: pyspark.sql.DataFrame,
    feature_domain_mapping,
) -> pyspark.sql.DataFrame:
    NUMBER_OF_HOURS_IN_A_WEEK = 24 * 7

    spark_session = SparkSession.builder.getOrCreate()

    df_timeliness = (
        df_timeliness.withColumn("layer", f.lit("master"))
        .withColumn("dimension", f.lit("timeliness"))
        .withColumn(
            "score",
            f.when(
                NUMBER_OF_HOURS_IN_A_WEEK >= f.col("delay"),
                (
                    f.round(
                        (
                            ((NUMBER_OF_HOURS_IN_A_WEEK - f.col("delay")) * 100)
                            / NUMBER_OF_HOURS_IN_A_WEEK
                        ),
                        3,
                    )
                ),
            ).otherwise(f.lit(0)),
        )
        .withColumn("unit", f.lit("percentage"))
    )

    domains = set()
    for column in df_master.columns:
        for prefix, domain in feature_domain_mapping.items():
            if column.startswith(prefix):
                domains.add(domain)

    domains_df = spark_session.createDataFrame(
        data=[[item] for item in domains],
        schema=StructType([StructField("domain", StringType(), False),]),
    )
    df_timeliness = df_timeliness.crossJoin(domains_df)
    df_timeliness = df_timeliness.select(
        [
            "score",
            "unit",
            "domain",
            "dimension",
            "master_mode",
            "table_name",
            "layer",
            "run_time",
        ]
    )
    return df_timeliness
