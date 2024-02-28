from pyspark.sql import SparkSession
from lib.logger import  Log4j
from lib.utils import get_local_dir
import pyspark.sql.functions as spark_functions
from pyspark.sql.types import IntegerType

import datetime as dt

def date_to_week_number(date):
    date = date.split()[0]
    day, month, year = map(int, date.split("-"))
    return dt.date(year, month, day).isocalendar()[1]


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

    logger.info("invoice_df schema: ")
    invoice_df.printSchema()
    invoice_df.select(spark_functions.count(spark_functions.expr("*")).alias("count *"),
                      spark_functions.sum("Quantity").alias("TotalQuantity"),
                      spark_functions.avg("UnitPrice").alias("AvgUnitPrice"),
                      spark_functions.countDistinct("InvoiceNo").alias("CountDistinctInvoiceNo")).show()

    invoice_df.selectExpr(
        "count(1) as count_1",
        "count(StockCode) as count_field",
        "sum(Quantity) as TotalQuantity",
        "avg(UnitPrice) as AvgUnitPrice"
    ).show()

    invoice_df.createOrReplaceTempView("sales")

    summary_df = spark.sql("""
        select Country, InvoiceNo, 
            sum(Quantity) as TotalQuantity,
            round(sum(Quantity * UnitPrice), 2) as InvoiceValue
        from sales
        group by Country, InvoiceNo
    """)
    summary_df.show()

    summary_df = invoice_df.groupBy("Country", "InvoiceNo") \
            .agg(spark_functions.sum("Quantity").alias("TotalQuantity"),
                spark_functions.round(spark_functions.sum(spark_functions.expr("Quantity * UnitPrice")), 2).alias("InvoiceValue"))
    date_to_week_number_udf = spark_functions.udf(date_to_week_number, IntegerType())

    exercise_df = invoice_df.withColumn("WeekNumber", date_to_week_number_udf("InvoiceDate")) \
        .groupBy("Country", "WeekNumber") \
        .agg(
            spark_functions.countDistinct("InvoiceNo").alias("NumInvoices"),
            spark_functions.sum("Quantity").alias("TotalQuantity"),
            spark_functions.round(spark_functions.sum(spark_functions.expr("Quantity * UnitPrice")), 2).alias("InvoiceValue")
        )
    exercise_df \
        .where(spark_functions.expr("Country = 'EIRE' or Country = 'France'")) \
        .where(exercise_df.WeekNumber >= 48) \
        .orderBy("Country", "WeekNumber").show()


    logger.info("spark application completed successfully")
    logger.info("stopping spark session...")
    spark.stop()
    logger.info("spark session stopped")

