from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def flatten_data(df: DataFrame):
    return (df
            .withColumn("report_location_type",
                        col("report_location.type"))
            .withColumn("report_location_long",
                        col("report_location.coordinates").getItem(0))
            .withColumn("report_location_lat",
                        col("report_location.coordinates").getItem(1))
            .drop("report_location"))
