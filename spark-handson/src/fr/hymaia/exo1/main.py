import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def wordcount(df, col_name):
    return df.withColumn('word', f.explode(f.split(f.col(col_name), ' '))) \
        .groupBy('word') \
        .count()
def main():

    #creation du job spark
    spark = SparkSession.builder \
        .appName("wordcount") \
        .master("local[*]") \
        .getOrCreate()

    #1. Lire le fichier `src/resources/exo1/data.csv`
    dfData = spark.read.option("header", "true").csv("src/resources/exo1/data.csv")

    #2. Appliquer la fonction `wordcount` à notre dataframe avec le bon nom de colonne
    dfWordCount = wordcount(dfData,"text")

    #3. On veut écrire le résultat dans `data/exo1/output` au format parquet
    #4. La donnée devra être partitionnée par "count"
    dfWordCount.write.partitionBy("count").parquet("data/exo1/output")
