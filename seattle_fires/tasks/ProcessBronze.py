from pyspark.sql.types import (ArrayType, DateType, DoubleType, StringType,
                               StructField, StructType)

from seattle_fires.utils.constants import (BRONZE_DATABASE, GOLD_DATABASE,
                                           SILVER_DATABASE)
from seattle_fires.utils.ReadTables import read_raw_data
from seattle_fires.utils.SaveTables import save_bronze_table
from seattle_fires.utils.SharedSparkInstance import SharedSparkInstance


def process_bronze() -> None:
    spark = SharedSparkInstance.get_instance()

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_DATABASE}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_DATABASE}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_DATABASE}")

    seattle_fires_schema = StructType([
        StructField("address", StringType()),
        StructField("type", StringType()),
        StructField("datetime", DateType()),
        StructField("latitude", StringType()),
        StructField("longitude", StringType()),
        StructField("report_location", StructType([
            StructField("type", StringType(), False),
            StructField("coordinates", ArrayType(DoubleType()), False)
        ]), False),
        StructField("incident_number", StringType()),
        StructField(":@computed_region_ru88_fbhk", StringType()),
        StructField(":@computed_region_kuhn_3gp2", StringType()),
        StructField(":@computed_region_q256_3sug", StringType()),

    ])

    # read from storage account
    df = read_raw_data(spark, seattle_fires_schema)
    # save as Delta Table
    save_bronze_table(df, "bronzeSeattleFires")

    spark.streams.awaitAnyTermination()
