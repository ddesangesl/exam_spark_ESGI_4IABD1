from pyspark.sql.functions import when, col, substring , count
from pyspark.sql import SparkSession

def calculatePopulationByDepartement(df):
    dfOrdered = df.orderBy(col("city"))
    dfPopulation = dfOrdered.groupBy("departement").agg(count("*").alias("nb"))
    return dfPopulation.orderBy(col("nb"), ascending=False)

def main():
    spark = SparkSession.builder \
        .appName("filterByNbPeople") \
        .master("local[*]") \
        .getOrCreate()

    #lecture des fichiers
    dfDepartement = spark.read.option("header", "true").parquet("data/exo2/output")

    dfNbPeople = calculatePopulationByDepartement(dfDepartement)
    dfNbPeople.write.mode("overwrite").csv("data/exo2/aggregate.csv")
