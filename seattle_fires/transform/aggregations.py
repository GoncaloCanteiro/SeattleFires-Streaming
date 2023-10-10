from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count


def count_types(df: DataFrame) -> DataFrame:
    return (df.select("type")
            .groupBy("type")
            .agg(count("type").alias("total"))
            .orderBy(col("total").desc()))


def count_days(df: DataFrame) -> DataFrame:
    return (df.select("datetime")
            .groupBy("datetime")
            .agg(count("datetime").alias("total"))
            .orderBy(col("total").desc()))


def count_location(df: DataFrame) -> DataFrame:
    return (df.select("latitude", "longitude")
            .groupBy(["latitude", "longitude"])
            .agg(count("latitude").alias("total"))
            .orderBy(col("total").desc()))
