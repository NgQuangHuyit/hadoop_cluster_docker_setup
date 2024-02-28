import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from lib.logger import Log4j
from lib.utils import get_local_dir


if __name__ == '__main__':

    conf = SparkConf() \
        .setAppName("Hello SparkSQL") \
        .setMaster("local[3]") \

    spark = SparkSession \
        .builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4j(spark)
    if len(sys.argv) != 2:
        logger.error("Usage: HelloRDD <file>")
        sys.exit(-1)

    logger.info("Starting HelloSparkSQL...")

    survey_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .format("csv") \
        .load(get_local_dir(sys.argv[1]))

    survey_df.createOrReplaceTempView('survey_tbl')
    count_df = spark.sql("select Country, count(*) as count from survey_tbl where age < 40 group by Country")
    count_df.show()

    spark.stop()
    logger.info("Finished HelloSparkSQL")