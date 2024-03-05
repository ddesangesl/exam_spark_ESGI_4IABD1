import unittest
from tests.fr.hymaia.spark_session_test import spark
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import pyspark.sql.functions as F
from src.fr.hymaia.exo4.scala_udf import addCategoryName


class TestMain(unittest.TestCase):
    def test_addColumnUdf(self):

        schema = StructType([StructField("category", StringType(), True)])
        data = [("5",), ("6",), ("7",)]
        input = spark.createDataFrame(data, schema=schema)

        schema = StructType([
        StructField("category", StringType(), True),
        StructField("category_name", StringType(), True)])
        data = [("5","food"), ("6","furniture"), ("7","furniture")]

        expected = spark.createDataFrame(data, schema=schema)   
        
        dfCateName = input.withColumn("category_name",addCategoryName(F.col("category")))


