from pyspark.sql import SparkSession
from lib.logger import  Log4j
from lib.utils import get_local_dir
import pyspark.sql.functions as spark_functions
from pyspark.sql.types import IntegerType


if __name__ == "__main__":
    spark = SparkSession.builder \
            .appName("AggDemo") \
            .master("local[3]") \
            .getOrCreate()

    logger = Log4j(spark)
    logger.info("spark session was created")
    logger.info("Starting AggDemo")

    invoice_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(get_local_dir("data/invoices.csv"))

    exSummary_df= invoice_df \
        .withColumn("InvoiceDate", spark_functions.to_date("InvoiceDate", "dd-MM-yyyy H.mm")) \
        .where("year(InvoiceDate) == 2010") \
        .withColumn("WeekNumber", spark_functions.weekofyear("InvoiceDate")) \
        .groupBy("Country","WeekNumber") \
        .agg(
            spark_functions.countDistinct("InvoiceNo").alias("NumInvoices"),
            spark_functions.sum("Quantity").alias("TotalQuantity"),
            spark_functions.round(spark_functions.sum(spark_functions.expr("Quantity * UnitPrice")), 2).alias("InvoiceValue")
        )

    exSummary_df.coalesce(1).write \
        .format("parquet") \
        .mode("overwrite") \
        .save(get_local_dir("output"))

    exSummary_df.sort("Country", "WeekNumber").show()
    spark.stop()
