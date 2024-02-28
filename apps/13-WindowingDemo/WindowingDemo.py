from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as sparkFunc
from lib.logger import Log4j
from lib.utils import get_local_dir

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("WindowingDemo") \
        .master("local[3]") \
        .getOrCreate()

    logger = Log4j(spark)
    logger.info("spark session was created")

    summary_df = spark.read \
        .format('parquet') \
        .load(get_local_dir("data/summary.parquet"))

    running_total_window = Window.partitionBy("Country").orderBy("WeekNumber") \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    summary_df.withColumn("RunningTotal", sparkFunc.sum("InvoiceValue").over(running_total_window)) \
        .show()

    spark.stop()