import sys
from collections import namedtuple

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from lib.logger import Log4j
from lib.utils import get_local_dir
import os
SurveyRecord = namedtuple("SurveyRecord", ["Age", "Gender", "Country", "state"])

if __name__ == '__main__':
    conf = SparkConf() \
        .setAppName("HelloRDD") \
        .setMaster("local[3]") \

    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4j(spark)
    sc = spark.sparkContext

    if len(sys.argv) != 2:
        logger.error("Usage: HelloRDD <file>")
        sys.exit(-1)

    linesRDD = sc.textFile(get_local_dir(sys.argv[1]))
    partitionedRDD = linesRDD.repartition(2)
    colsRDD = partitionedRDD.map(lambda line: line.replace('"', '').split(','))
    selectRDD = colsRDD.map(lambda cols: SurveyRecord(int(cols[1]), cols[2], cols[3], cols[4]))
    filteredRDD = selectRDD.filter(lambda record: record.Age < 40)
    kvRDD = filteredRDD.map(lambda record: (record.Country, 1))
    countRDD = kvRDD.reduceByKey(lambda v1, v2: v1 + v2)

    colslist = countRDD.collect()
    for x in colslist:
        logger.info(x)

    spark.stop()
