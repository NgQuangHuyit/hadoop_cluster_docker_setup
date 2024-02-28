from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, expr
from pyspark.sql.types import StringType

from lib.logger import Log4j
from lib.utils import get_local_dir
import re


def parse_gender(gender):
    female_pattern = r"^f$|f.m|w.m"
    male_pattern = r"m$|ma|m.l"

    if re.search(female_pattern, gender.lower()):
        return "Female"
    elif re.search(male_pattern, gender.lower()):
        return "Male"
    else:
        return "Unknown"

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("UDF_Demo") \
        .master("local[3]") \
        .getOrCreate()

    logger = Log4j(spark)
    survey_df = spark.read \
            .format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(get_local_dir("data/survey.csv"))
    logger.info(f"Survey Data Schema: {survey_df.schema.simpleString()}")
    survey_df.show(10)


    parse_gender_udf = udf(parse_gender, StringType())
    transformed_survey_df = survey_df.withColumn("Gender", parse_gender_udf("Gender"))

    for f in spark.catalog.listFunctions():
        if "parse_gender_udf" in f.name:
            logger.info(f"Function: {f.name} is registered")

    logger.info(f"Survey Data Schema: {transformed_survey_df.schema.simpleString()}")
    transformed_survey_df.show()

    spark.udf.register("parse_gender_udf", parse_gender, StringType())
    for f in spark.catalog.listFunctions():
        if "parse_gender_udf" in f.name:
            logger.info(f"Function: {f.name} is registered")

    transformed_survey_df2 = survey_df.withColumn("Gender", expr("parse_gender_udf(Gender) as formattedGender"))
    transformed_survey_df2.show()
    spark.stop()
