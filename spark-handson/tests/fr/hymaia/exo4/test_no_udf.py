import unittest
from tests.fr.hymaia.spark_session_test import spark
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import pyspark.sql.functions as F
from src.fr.hymaia.exo4.no_udf import addColumn, add_total_price_per_category_per_day_col, add_total_price_per_category_per_day_last_30_days_col
from pyspark.sql import Row

class TestMain(unittest.TestCase):
    def test_addColumn(self):

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
           
        
        dfCateName = addColumn(input)
        self.assertCountEqual(expected.collect(), dfCateName.collect())

    def test_add_total_price_per_category_per_day_col(self):

        input = spark.createDataFrame(
            [
                Row(date='2019-02-17',price='10',category='1'),
                Row(date='2019-02-17',price='10',category='1'),
                Row(date='2019-02-17',price='10',category='2'),
                Row(date='2019-02-18',price='20',category='2'),
                Row(date='2019-02-18',price='20',category='2'),
                Row(date='2019-02-18',price='20',category='1')

            ]
        )


        expected = spark.createDataFrame(
            [
                Row(date='2019-02-17',price='10',category='1',total_price_per_category_per_day=20),
                Row(date='2019-02-17',price='10',category='1',total_price_per_category_per_day=20),
                Row(date='2019-02-17',price='10',category='2',total_price_per_category_per_day=10),
                Row(date='2019-02-18',price='20',category='2',total_price_per_category_per_day=40),
                Row(date='2019-02-18',price='20',category='2',total_price_per_category_per_day=40),
                Row(date='2019-02-18',price='20',category='1',total_price_per_category_per_day=20)

            ]
        ) 
        
        input = add_total_price_per_category_per_day_col(input)
        self.assertCountEqual(expected.collect(), input.collect())

    def test_add_total_price_per_category_per_day_last_30_days_col(self):
        self.maxDiff = None
        input = spark.createDataFrame(
            [
                Row(date='2019-05-23',price='10',category='1'),
                Row(date='2019-06-20',price='20',category='1'),
                Row(date='2019-07-18',price='20',category='1'),
                Row(date='2019-05-23',price='10',category='2'),
                Row(date='2019-06-20',price='10',category='2'),
                Row(date='2019-07-18',price='20',category='2')

            ]
        )


        expected = spark.createDataFrame(
            [
                Row(date='2019-05-23',price='10',category='1',total_prix_30jours=10),
                Row(date='2019-06-20',price='20',category='1',total_prix_30jours=30),
                Row(date='2019-07-18',price='20',category='1',total_prix_30jours=40),

                Row(date='2019-05-23',price='10',category='2',total_prix_30jours=10),
                Row(date='2019-06-20',price='10',category='2',total_prix_30jours=20),
                Row(date='2019-07-18',price='20',category='2',total_prix_30jours=30)

            ]
        ) 
        
        input = add_total_price_per_category_per_day_last_30_days_col(input)
        self.assertCountEqual(input.collect(), expected.collect())


