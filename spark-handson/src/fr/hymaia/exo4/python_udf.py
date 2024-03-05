import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("exo4_python").master("local[*]").getOrCreate()

@F.udf('string')
def addColumnUdf(col_name):
    if int(col_name) < 6:
        return "food"
    else:
        return "furniture"

def main():
    dfSell = spark.read.option("header", "true").csv("src/resources/exo4/sell.csv")
    dfCateName = dfSell.withColumn("category_name",addColumnUdf(F.col("category")))