from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id

from lib.logger import Log4j
from lib.utils import get_local_dir

if __name__ == "__main__":
    conf = SparkConf() \
        .setAppName("SparkSchemaDemo") \
        .setMaster("local[2]") \
        .set("spark.jars.packages", "org.apache.spark:spark-avro_2.13:3.4.2") \

    spark = SparkSession \
        .builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4j(spark)
    logger.info("Starting SparkSchemaDemo...")

    flightTimeDF = spark.read \
        .format("parquet") \
        .load(get_local_dir("data-source/flight*.parquet"))

    flightTimeDF.write \
    .format("avro") \
    .mode("overwrite") \
    .option("path", get_local_dir("data-sink/avro")) \
    .save()

    logger.info("Number of partitions in avro file: " + str(flightTimeDF.rdd.getNumPartitions()))
    flightTimeDF.groupBy(spark_partition_id()).count().show()
    spark.stop()
    logger.info("Spark application finished")