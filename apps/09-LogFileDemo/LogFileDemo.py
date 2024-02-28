from pyspark.sql import *
from pyspark.sql.functions import regexp_extract, substring_index
from lib.utils import get_local_dir
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("LogFileDemo") \
        .master("local[3]") \
        .getOrCreate()

    file_df = spark.read.text(get_local_dir("data/apache_logs.txt"))
    file_df.printSchema()

    log_regx = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)'
    logs_df = file_df.select(regexp_extract("value", log_regx, 1).alias("host"),
                   regexp_extract("value", log_regx, 4).alias("date"),
                  regexp_extract("value", log_regx, 6).alias("request"),
                  regexp_extract("value", log_regx, 10).alias("referrer"))

    logs_df.printSchema()
    logs_df.show(15)

    logs_df \
        .where("trim(referrer) != '-'") \
        .withColumn("referrer", substring_index("referrer", "/", 3)) \
        .groupBy("referrer") \
        .count() \
        .show(10, False)