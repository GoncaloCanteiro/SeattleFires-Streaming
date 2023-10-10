from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType


def cast_data(df: DataFrame):
    return (df
            .withColumn("latitude",
                        col("latitude").cast(DoubleType()))
            .withColumn("longitude",
                        col("longitude").cast(DoubleType()))
            .withColumn("report_location_type",
                        col("report_location_type"))
            .withColumn("report_location_long",
                        col("report_location_long").cast(DoubleType()))
            .withColumn("report_location_lat",
                        col("report_location_lat").cast(DoubleType())))
