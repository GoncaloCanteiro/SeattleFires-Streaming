import time

from seattle_fires.transform.castData import cast_data
from seattle_fires.transform.flattenData import flatten_data
from seattle_fires.utils.constants import BRONZE_DATABASE, CATALOG_NAME
from seattle_fires.utils.ReadTables import read_delta_bronze
from seattle_fires.utils.SaveTables import save_silver_table
from seattle_fires.utils.SharedSparkInstance import SharedSparkInstance
from seattle_fires.utils.tableExists import check_table_exists


def process_silver() -> None:
    spark = SharedSparkInstance.get_instance()

    isTableExist = check_table_exists(BRONZE_DATABASE, "bronzeSeattleFires", spark)
    ts = time.time()

    if isTableExist:
        print(f"Silver: Table exist {ts}")
        df_bronze = read_delta_bronze(spark, CATALOG_NAME,
                                      BRONZE_DATABASE, "bronzeSeattleFires")

        if df_bronze:
            print("Silver: Flatten data")

            df_flatten = flatten_data(df_bronze)

            df_cast = cast_data(df_flatten)

            # save as Delta Table
            save_silver_table(df_cast, "silverSeattleFires")

    else:
        print(f"Silver: Bronze does not exist {ts}")
