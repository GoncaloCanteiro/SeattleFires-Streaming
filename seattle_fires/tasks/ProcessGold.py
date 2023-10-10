import time

from seattle_fires.transform.aggregations import (count_days, count_location,
                                                  count_types)
from seattle_fires.utils.constants import CATALOG_NAME, SILVER_DATABASE
from seattle_fires.utils.ReadTables import read_delta_silver
from seattle_fires.utils.SaveTables import (save_gold_count_days_table,
                                            save_gold_count_types_table,
                                            save_gold_location_table)
from seattle_fires.utils.SharedSparkInstance import SharedSparkInstance
from seattle_fires.utils.tableExists import check_table_exists


def process_gold() -> None:
    spark = SharedSparkInstance.get_instance()

    isTableExist = check_table_exists(SILVER_DATABASE, "silverSeattleFires", spark)
    ts = time.time()

    if isTableExist:
        print(f"Silver: Table exist {ts}")

        df_silver = read_delta_silver(spark, CATALOG_NAME,
                                      SILVER_DATABASE, "silverSeattleFires")

        # save as Gold Delta Table types
        df_types = count_types(df_silver)

        save_gold_count_types_table(df_types, "goldSeattleFiresTypes")

        # save as Gold Delta Table Days
        df_days = count_days(df_silver)

        save_gold_count_days_table(df_days, "goldSeattleFiresDays")

        # save as Gold Delta Table Location
        df_location = count_location(df_silver)

        save_gold_location_table(df_location, "goldSeattleFiresLocation")
    else:
        print(f"GOLD: Silver does not exist {ts}")

    spark.streams.awaitAnyTermination()
