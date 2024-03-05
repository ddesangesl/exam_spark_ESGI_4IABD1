import unittest
from tests.fr.hymaia.spark_session_test import spark
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import pyspark.sql.functions as F
from pyspark.sql import Row

from src.fr.hymaia.exo4.python_udf import addColumnUdf

class TestMain(unittest.TestCase):
    def test_addColumnUdf(self):

        schema = StructType([StructField("category", StringType(), True)])
        data = [("5",), ("6",), ("7",)]

        input = spark.createDataFrame(
            [
                Row(category='5'),
                Row(category='6'),
                Row(category='7')

            ]
        )

        expected = spark.createDataFrame(
            [
                Row(category='5',category_name='food'),
                Row(category='6',category_name='furniture'),
                Row(category='7',category_name='furniture'),
            ]
        )
        dfCateName = input.withColumn("category_name",addColumnUdf(F.col("category")))

        self.assertCountEqual(expected.collect(), dfCateName.collect())


