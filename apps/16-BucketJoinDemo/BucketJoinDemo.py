from pyspark.sql import SparkSession
from lib.logger import Log4j
from lib.utils import get_local_dir


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("BucketJoinDemo") \
        .master("local[3]") \
        .enableHiveSupport() \
        .getOrCreate()

    logger = Log4j(spark)

    df1 = spark.read.json(get_local_dir("data/d1/"))
    df2 = spark.read.json(get_local_dir("data/d2/"))

    # spark.sql("CREATE DATABASE IF NOT EXISTS MY_DB")
    # df1.coalesce(1).write \
    #     .bucketBy(3, "id") \
    #     .sortBy("id") \
    #     .mode("overwrite") \
    #     .saveAsTable("MY_DB.flight_data1")
    #
    # df2.coalesce(1).write \
    #     .bucketBy(3, "id") \
    #     .sortBy("id") \
    #     .mode("overwrite") \
    #     .saveAsTable("MY_DB.flight_data2")

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    table_1 = spark.read.table("MY_DB.flight_data1")
    table_2 = spark.read.table("MY_DB.flight_data2")

    join_expr = table_1.id == table_2.id
    joined_df = table_1.join(table_2, join_expr, "inner")
    joined_df.collect()
    input("Press Enter to continue...")
    spark.stop()