import datetime
from unittest import TestCase

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, Row
from RowDemo import to_date_df

class RowDemoTest(TestCase):

    @classmethod
    def setUpClass(cls) :
        cls.spark = SparkSession \
            .builder \
            .appName("RowDemoTest") \
            .master("local[3]") \
            .getOrCreate()

        my_schema = StructType([
            StructField("ID", StringType()),
            StructField("EventDate", StringType())])
        my_rows = [
            Row("123", "04/05/2020"),
            Row("124", "4/5/2020"),
            Row("125", "04/5/2020"),
            Row("126", "4/05/2020")]

        cls.my_df = cls.spark.createDataFrame(my_rows, my_schema)

    def test_data_type(self):
        rows = to_date_df(self.my_df, "M/d/y", "EventDate").collect()
        for row in rows:
            self.assertIsInstance(row.EventDate, datetime.date)

    def test_data_value(self):
        rows = to_date_df(self.my_df, "M/d/y", "EventDate").collect()
        for row in rows:
            self.assertEqual(row.EventDate, datetime.date(2020, 4, 5))
