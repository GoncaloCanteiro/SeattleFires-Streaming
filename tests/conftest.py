import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.master("Test Session")
        .config("spark.port.maxRetries", "1000")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    return spark
