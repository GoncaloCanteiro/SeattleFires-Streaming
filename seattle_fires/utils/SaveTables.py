from pyspark.sql import DataFrame

from seattle_fires.utils.constants import (BRONZE_DATABASE, CATALOG_NAME,
                                           GOLD_DATABASE, SILVER_DATABASE,
                                           STREAM_TIME)


def save_bronze_table(df: DataFrame, table_name: str):
    (df.writeStream
     .format("delta")
     .outputMode("append")
     .option("mergeSchema", True)
     .option("checkpointLocation",
             f"dbfs:/FileStore/streaming_gc/checkpoints/{BRONZE_DATABASE}/{table_name}/")
     .trigger(processingTime=f'{STREAM_TIME} seconds')
     .toTable(f"{CATALOG_NAME}.{BRONZE_DATABASE}.{table_name}"))


def save_silver_table(df: DataFrame, table_name: str):
    (df.writeStream
     .format("delta")
     .outputMode("append")
     .option("mergeSchema", True)
     .option("checkpointLocation",
             f"dbfs:/FileStore/streaming_gc/checkpoints/{SILVER_DATABASE}/{table_name}/")
     .trigger(processingTime=f'{STREAM_TIME} seconds')
     .toTable(f"{CATALOG_NAME}.{SILVER_DATABASE}.{table_name}"))


def save_gold_count_types_table(df: DataFrame, table_name: str):
    (df.writeStream
     .format("delta")
     .outputMode("complete")
     .option("mergeSchema", True)
     .option("checkpointLocation",
             f"dbfs:/FileStore/streaming_gc/checkpoints/{GOLD_DATABASE}/{table_name}/")
     .trigger(processingTime=f'{STREAM_TIME} seconds')
    .toTable(f"{CATALOG_NAME}.{GOLD_DATABASE}.{table_name}"))



def save_gold_count_days_table(df: DataFrame, table_name: str):
    (df.writeStream
     .format("delta")
     .outputMode("complete")
     .option("mergeSchema", True)
     .option("checkpointLocation",
             f"dbfs:/FileStore/streaming_gc/checkpoints/{GOLD_DATABASE}/{table_name}/")
     .trigger(processingTime=f'{STREAM_TIME} seconds')
     .toTable(f"{CATALOG_NAME}.{GOLD_DATABASE}.{table_name}"))



def save_gold_location_table(df: DataFrame, table_name: str):
    (df.writeStream
     .format("delta")
     .outputMode("complete")
     .option("mergeSchema", True)
     .option("checkpointLocation",
             f"dbfs:/FileStore/streaming_gc/checkpoints/{GOLD_DATABASE}/{table_name}/")
     .trigger(processingTime=f'{STREAM_TIME} seconds')
     .toTable(f"{CATALOG_NAME}.{GOLD_DATABASE}.{table_name}"))
