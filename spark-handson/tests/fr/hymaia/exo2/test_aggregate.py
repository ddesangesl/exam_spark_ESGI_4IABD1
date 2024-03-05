from tests.fr.hymaia.spark_test_case import spark
import unittest
from src.fr.hymaia.exo2.aggregate import calculatePopulationByDepartement
from pyspark.sql import Row

class TestMain(unittest.TestCase):
    def test_calculatePopulationByDepartement(self):
        # GIVEN
        inputDepartement = spark.createDataFrame(
            [
                Row(name='G',age='67',zip='49460',city='villeA',departement='49'),
                Row(name='A',age='67',zip='49460',city='villeA',departement='49'),
                Row(name='B',age='68',zip='75016',city='villeB',departement='75'),
                Row(name='C',age='69',zip='20190',city=None,departement='2A'),
                Row(name='D',age='70',zip='20191',city=None,departement='2B'),
                Row(name='E',age='70',zip='20191',city=None,departement='2B'),
                Row(name='F',age='70',zip='20191',city=None,departement='2B')

            ]
        )

        expected = spark.createDataFrame(
            [
                Row(departement='2B',nb=3),
                Row(departement='49',nb=2),
                Row(departement='75',nb=1),
                Row(departement='2A',nb=1)
            ]
        )

        actual = calculatePopulationByDepartement(inputDepartement)

        self.assertCountEqual(actual.collect(), expected.collect())

    def test_no_column_departement(self):
        inputDepartement = spark.createDataFrame(
            [
                Row(name='G',age='67',zip='49460',city='villeA'),
                Row(name='A',age='67',zip='49460',city='villeA'),
                Row(name='B',age='68',zip='75016',city='villeB'),
                Row(name='C',age='69',zip='20190',city=None),
                Row(name='D',age='70',zip='20191',city=None),
                Row(name='E',age='70',zip='20191',city=None),
                Row(name='F',age='70',zip='20191',city=None)

            ]
        )

        with self.assertRaises(Exception):
            calculatePopulationByDepartement(inputDepartement)

