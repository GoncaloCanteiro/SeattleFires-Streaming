from pyspark.sql import SparkSession


def check_table_exists(db_name: str, table_name: str, spark: SparkSession):
    return spark.catalog.tableExists(f"{db_name}.{table_name}")
