from pyspark.sql import SparkSession
from lib.utils import get_local_dir
from lib.logger import Log4j

if __name__ == "__main__":

    spark = SparkSession.builder \
            .appName("SparkSQLTableDemo") \
            .master("local[3]") \
            .enableHiveSupport() \
            .getOrCreate()

    logger = Log4j(spark)
    logger.info("Starting Spark SQL Table Demo")

    flightTimeParquetDF = spark.read \
        .format("parquet") \
        .option("path", get_local_dir("data-source/flight-*.parquet")) \
        .load()

    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")

    spark.catalog.setCurrentDatabase("AIRLINE_DB")
    flightTimeParquetDF.write \
        .format("csv") \
        .mode("overwrite") \
        .bucketBy(5, "OP_CARRIER", "ORIGIN") \
        .sortBy("OP_CARRIER", "ORIGIN") \
        .saveAsTable("flight_data_tbl")

    logger.info(spark.catalog.listTables("AIRLINE_DB"))
    spark.stop()
    logger.info("Spark SQL Table Demo Application Completed")

