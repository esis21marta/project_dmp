import pyspark
import pyspark.sql.functions as f


def availability_score(
    df: pyspark.sql.DataFrame, pipeline: str
) -> pyspark.sql.DataFrame:
    df_score = df.groupby(["domain", "table_name", "run_time"]).agg(
        f.sum("is_missing").alias("score")
    )
    df_score = (
        df_score.withColumn("unit", f.lit("missing count"))
        .withColumn("master_mode", f.lit(pipeline))
        .withColumn("layer", f.lit("source"))
        .withColumn("dimension", f.lit("availability"))
    )
    return df_score
