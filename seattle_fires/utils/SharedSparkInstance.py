import pyspark
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from seattle_fires.utils.constants import CATALOG_NAME

def get_dbutils(spark):
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
    except ImportError:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
    return dbutils

class SharedSparkInstance:
    @staticmethod
    def get_instance() -> SparkSession:
        builder = (pyspark.sql.SparkSession.builder.appName("Streaming Delta App")
                   .config("spark.sql.extensions",
                           "io.delta.sql.DeltaSparkSessionExtension")
                   .config("spark.sql.catalog.spark_catalog",
                           "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
        #           .config("spark.sql.warehouse.dir", "spark-warehouse"))

        spark = configure_spark_with_delta_pip(builder).getOrCreate()

        dbutils = get_dbutils(spark)

        myAzureKey = dbutils.secrets.get("SecretBucketGC", "azureAccountKey")
        myAzurePass = dbutils.secrets.get("SecretBucketGC", "azureAccountPass")

        spark.conf.set(myAzureKey, myAzurePass)

        spark.sql("USE CATALOG learning_instance")

        return spark
