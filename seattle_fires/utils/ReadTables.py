from delta.tables import *

from seattle_fires.utils.constants import STORAGE_PATH, STORAGE_RAW_PATH


def read_raw_data(spark: SparkSession, schema):
    df = ((spark
           .readStream
           .schema(schema)
           .json(STORAGE_PATH + STORAGE_RAW_PATH))
    )
    return df


def read_delta_bronze(spark: SparkSession, catalog: str, database: str, table_name: str) -> DataFrame:
    bronze_df = (spark.readStream
                 .format("delta")
                 .option("ignoreChanges", "true")
                 .table(f"{catalog}.{database}.{table_name}"))
    return bronze_df


def read_delta_silver(spark: SparkSession, catalog: str, database: str, table_name: str) -> DataFrame:
    return (spark.readStream
            .format("delta")
            .option("ignoreChanges", "true")
            .table(f"{catalog}.{database}.{table_name}"))
