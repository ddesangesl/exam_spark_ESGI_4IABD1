
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("exo4_python").master("local[*]").getOrCreate()

def addColumn(dataFrame):
    categoryName = F.when(F.col("category") < 6 , "food").otherwise("furniture")
    return dataFrame.withColumn("category_name",categoryName)

def add_total_price_per_category_per_day_col(df):
    windowSpec  = Window.partitionBy("date","category")
    return df.withColumn("total_price_per_category_per_day",F.sum("price").over(windowSpec))

def add_total_price_per_category_per_day_last_30_days_col(df):

    return df.groupBy('category', F.window('date', '30 days').alias('window')) \
           .agg(F.sum('price').alias('total_prix_30jours'))

def main():
    dfSell = spark.read.option("header", "true").csv("src/resources/exo4/sell.csv")
    dfCateName = addColumn(dfSell)
    
    dfPriceTotal = add_total_price_per_category_per_day_col(dfCateName)
    dfPriceTotal = add_total_price_per_category_per_day_last_30_days_col(dfPriceTotal)
    