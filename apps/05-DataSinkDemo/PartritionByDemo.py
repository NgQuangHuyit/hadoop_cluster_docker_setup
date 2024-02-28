from pyspark import SparkConf
from pyspark.sql import SparkSession
from lib.utils import get_local_dir
from lib.logger import Log4j

if __name__ == "__main__":
    conf = SparkConf() \
        .setAppName("Partition By Demo") \
        .setMaster("local[3]") \

    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()
    logger = Log4j(spark)

    logger.info("SparkSession created")
    logger.info("Starting PartitionByDemo...")

    # flightTimeDF = spark.read \
    #     .format("parquet") \
    #     .load(get_local_dir("data-source/flight-*.parquet"))
    #
    # flightTimeDF.show(10)
    # flightTimeDF.groupBy("OP_CARRIER", "ORIGIN").agg({"DEP_TIME": "avg", "DISTANCE": "sum"} ).show(10)

    flightTimeDF = spark.read \
        .format("parquet") \
        .load(get_local_dir("data-source/flight*.parquet"))

    flightTimeDF.write \
        .format("json") \
        .mode("overwrite") \
        .option("path", get_local_dir("data-sink/partitioned")) \
        .partitionBy("OP_CARRIER", "ORIGIN") \
        .save()

    spark.stop()
    logger.info("Spark application finished")