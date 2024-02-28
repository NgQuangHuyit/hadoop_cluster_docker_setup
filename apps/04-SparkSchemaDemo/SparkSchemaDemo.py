from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType

from lib.logger import Log4j
from lib.utils import get_local_dir


if __name__ == "__main__":
    conf = SparkConf() \
        .setAppName("SparkSchemaDemo") \
        .setMaster("local[3]") \

    spark = SparkSession \
        .builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4j(spark)
    logger.info("Starting SparkSchemaDemo...")

    flightSchemaStruct = StructType([
        StructField("FL_DATE", DateType()),
        StructField("OP_CARRIER", StringType()),
        StructField("OP_CARRIER_FL_NUM", IntegerType()),
        StructField("ORIGIN", StringType()),
        StructField("ORIGIN_CITY_NAME", StringType()),
        StructField("DEST", StringType()),
        StructField("DEST_CITY_NAME", StringType()),
        StructField("CRS_DEP_TIME", IntegerType()),
        StructField("DEP_TIME", IntegerType()),
        StructField("WHEELS_ON", IntegerType()),
        StructField("TAXI_IN", IntegerType()),
        StructField("CRS_ARR_TIME", IntegerType()),
        StructField("ARR_TIME", IntegerType()),
        StructField("CANCELLED", IntegerType()),
        StructField("DISTANCE", IntegerType()),
    ])

    flightTimeCsvDF = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("dateFormat", "M/d/y") \
        .option("mode", "FAILFAST") \
        .schema(flightSchemaStruct) \
        .load(get_local_dir("data/flight*.csv"))

    flightTimeCsvDF.show(5)

    logger.info(f"Csv Schema: {flightTimeCsvDF.schema.simpleString()}")

    flightSchemaDDL ="""
        FL_DATE DATE,
        OP_CARRIER STRING,
        OP_CARRIER_FL_NUM INT,
        ORIGIN STRING,
        ORIGIN_CITY_NAME STRING,
        DEST STRING,
        DEST_CITY_NAME STRING,
        CRS_DEP_TIME INT,
        DEP_TIME INT,
        WHEELS_ON INT,
        TAXI_IN INT,
        CRS_ARR_TIME INT,
        ARR_TIME INT,
        CANCELLED INT,
        DISTANCE INT"""

    flightTimeJsonDF = spark.read \
        .format("json") \
        .option("dateFormat", "M/d/y") \
        .option("mode", "FAILFAST") \
        .schema(flightSchemaDDL) \
        .load(get_local_dir("data/flight*.json"))

    flightTimeJsonDF.show(5)
    logger.info(flightTimeJsonDF.count())
    logger.info(f"Json Schema: {flightTimeJsonDF.schema.simpleString()}")
    spark.stop()
    logger.info("Finished SparkSchemaDemo")

