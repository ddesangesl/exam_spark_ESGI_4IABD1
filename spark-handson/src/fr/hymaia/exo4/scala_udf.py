import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.column import Column, _to_java_column, _to_seq

spark = SparkSession.builder.appName("exo4_scala")\
        .master("local[*]")\
        .config('spark.jars', 'src/resources/exo4/udf.jar')\
        .getOrCreate()

def addCategoryName(col):
    # on récupère le SparkContext
    sc = spark.sparkContext
    # Via sc._jvm on peut accéder à des fonctions Scala
    add_category_name_udf = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()
    # On retourne un objet colonne avec l'application de notre udf Scala
    return Column(add_category_name_udf.apply(_to_seq(sc, [col], _to_java_column)))

def main():
    dfSell = spark.read.option("header", "true").csv("src/resources/exo4/sell.csv")
    dfCateName = dfSell.withColumn("category_name",addCategoryName(F.col("category")))