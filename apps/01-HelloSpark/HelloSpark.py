import sys
from pyspark.sql import SparkSession
from lib.logger import Log4j
from os.path import join
from os import getcwd
from lib.utils import load_survey_df

if __name__ == "__main__":
    spark = SparkSession.builder \
            .master('local[3]') \
            .appName("spark application").getOrCreate()

    logger = Log4j(spark)
    logger.info("Starting HelloSpark")
    if len(sys.argv) != 2:
        logger.error("Usage: HelloSpark <filename>")
        sys.exit(-1)

    survey_df = load_survey_df(spark, f"file://{join(getcwd(), sys.argv[1])}").repartition(2)
    count_df = survey_df \
        .filter("Age < 40") \
        .select("Age", "Gender", "Country", "state") \
        .groupBy("Country") \
        .count()
    logger.info(count_df.collect())
    logger.info("Finished HelloSpark")
    spark.stop()
    