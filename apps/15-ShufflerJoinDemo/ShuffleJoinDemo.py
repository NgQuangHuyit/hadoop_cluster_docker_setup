from pyspark.sql import SparkSession
from lib.logger import Log4j
from lib.utils import get_local_dir
from pyspark.sql.functions import broadcast

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("ShuffleJoinDemo") \
        .master("local[3]") \
        .getOrCreate()

    flight_time_df1 = spark.read.json(get_local_dir("data/d1/"))
    flight_time_df2 = spark.read.json(get_local_dir("data/d2/"))

    spark.conf.set("spark.sql.shuffle.partitions", 3)
    join_expr = flight_time_df1.id == flight_time_df2.id

    joined_df = flight_time_df1.join(broadcast(flight_time_df2), join_expr, "inner")

    joined_df.foreach(lambda x: None)
    input("press any key to stop the application...")