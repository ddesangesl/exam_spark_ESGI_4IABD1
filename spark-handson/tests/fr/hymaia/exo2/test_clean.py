from tests.fr.hymaia.spark_test_case import spark
import unittest
from src.fr.hymaia.exo2.clean import filterByAge ,joinDFByZip,dfWithDepartement , clean
from pyspark.sql import Row

class TestMain(unittest.TestCase):
    def test_filterByAge(self):
        # GIVEN
        input = spark.createDataFrame(
            [
                Row(nom='A',age='45'),
                Row(nom='B',age='12'),
                Row(nom='C',age='18'),
                Row(nom='D',age='0')
            ]
        )
        expected = spark.createDataFrame(
            [
                Row(nom='A',age='45'),
                Row(nom='C',age='18')
            ]
        )

        actual = filterByAge(input)

        self.assertCountEqual(actual.collect(), expected.collect())
    
    def test_no_column_age(self):
        input = spark.createDataFrame(
            [
                Row(nom='A'),
                Row(nom='B'),
                Row(nom='C'),
                Row(nom='D')
            ]
        )

        with self.assertRaises(Exception):
            filterByAge(input)
        
    def test_joinDFByZip(self):
        # GIVEN
        inputClient = spark.createDataFrame(
            [
                Row(name='A',age='67',zip='49460'),
                Row(name='B',age='68',zip='75016'),
                Row(name='C',age='69',zip='56778'),
                Row(name='D',age='70',zip='23568')
            ]
        )
        inputVille = spark.createDataFrame(
            [
                Row(city='villeA',zip='49460'),
                Row(city='villeB',zip='75016'),
                Row(city='villeC',zip='18689'),
                Row(city='villeD',zip='56876')
            ]
        )
        expected = spark.createDataFrame(
            [
                Row(name='A',age='67',zip='49460',city='villeA'),
                Row(name='B',age='68',zip='75016',city='villeB'),
                Row(name='C',age='69',zip='56778',city=None),
                Row(name='D',age='70',zip='23568',city=None)
            ]
        )

        actual = joinDFByZip(inputClient,inputVille)

        self.assertCountEqual(actual.collect(), expected.collect())

    def test_no_column_zip(self):
        inputClient = spark.createDataFrame(
            [
                Row(name='A',age='67'),
                Row(name='B',age='68'),
                Row(name='C',age='69'),
                Row(name='D',age='70')
            ]
        )
        inputVille = spark.createDataFrame(
            [
                Row(city='villeA',zip='49460'),
                Row(city='villeB',zip='75016'),
                Row(city='villeC',zip='18689'),
                Row(city='villeD',zip='56876')
            ]
        )

        with self.assertRaises(Exception):
            joinDFByZip(inputClient,inputVille)

    def test_dfWithDepartement(self):
        # GIVEN
        inputDepartement = spark.createDataFrame(
            [
                Row(name='A',age='67',zip='49460',city='villeA'),
                Row(name='B',age='68',zip='75016',city='villeB'),
                Row(name='C',age='69',zip='20190',city=None),
                Row(name='D',age='70',zip='20191',city=None)
            ]
        )

        expected = spark.createDataFrame(
            [
                Row(name='A',age='67',zip='49460',city='villeA',departement='49'),
                Row(name='B',age='68',zip='75016',city='villeB',departement='75'),
                Row(name='C',age='69',zip='20190',city=None,departement='2A'),
                Row(name='D',age='70',zip='20191',city=None,departement='2B')
            ]
        )

        actual = dfWithDepartement(inputDepartement)

        self.assertCountEqual(actual.collect(), expected.collect())
    
    def test_clean(self):
        # GIVEN
        inputClient = spark.createDataFrame(
            [
                Row(name='A',age='67',zip='49460'),
                Row(name='B',age='68',zip='75016'),
                Row(name='C',age='69',zip='56778'),
                Row(name='D',age='70',zip='23568'),
                Row(name='E',age='17',zip='55550')
            ]
        )
        inputVille = spark.createDataFrame(
            [
                Row(city='villeA',zip='49460'),
                Row(city='villeB',zip='75016'),
                Row(city='villeC',zip='18689'),
                Row(city='villeD',zip='56876'),
                Row(city='villeD',zip='55550')
            ]
        )
        expected = spark.createDataFrame(
            [
                Row(name='A',age='67',zip='49460',city='villeA',departement='49'),
                Row(name='B',age='68',zip='75016',city='villeB',departement='75'),
                Row(name='C',age='69',zip='56778',city=None,departement='56'),
                Row(name='D',age='70',zip='23568',city=None,departement='23')
            ]
        )

        actual = clean(inputClient,inputVille)

        self.assertCountEqual(actual.collect(), expected.collect())
