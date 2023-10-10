import requests
from pyspark.sql import SparkSession
import pyspark
from delta import configure_spark_with_delta_pip

def get_dbutils(spark):
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
    except ImportError:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
    return dbutils

spark = SparkSession.builder \
    .master('yarn') \
    .appName('pythonSpark') \
    .getOrCreate()

dbutils = get_dbutils(spark)

#variable declarations
myAzureKey = dbutils.secrets.get("SecretBucketGC", "azureAccountKey")
myAzurePass = dbutils.secrets.get("SecretBucketGC", "azureAccountPass")
storage_path = dbutils.secrets.get("SecretBucketGC", "azureStoragePath")

spark.conf.set(myAzureKey, myAzurePass)

url = 'https://data.seattle.gov/resource/kzjm-xkqj.json'
file = 'sfd-'
extension = '.json'

def get_data(api):
    response = requests.get(f"{api}")
    if response.status_code == 200:
        return response.json()
    else:
        print(f"There's a {response.status_code} error with your request")


def ___save_json_api_to_file(api: str, file: str, extension: str):
    import time

    #while True:
    json_array = get_data(api)
    timestr = time.strftime("%Y-%m-%d")

    rdd = spark.sparkContext.parallelize(json_array)
    df = spark.read.json(rdd)

    print(timestr)
    df.write.format("json") \
        .mode("append") \
        .save(storage_path + f"/gc/raw/{file}{timestr}")

        #sleep(300)


def main():
    ___save_json_api_to_file(url, file, extension)


if __name__ == '__main__':
    main()
