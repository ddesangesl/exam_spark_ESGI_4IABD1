from pyspark.sql.functions import when, col, substring
from pyspark.sql import SparkSession

def filterByAge(df):
    return df.where(col("age") >= 18)

def joinDFByZip(dfClient,dfCity):
    dfjoin = dfClient.join(dfCity,"zip","left")
    return dfjoin.select("name","age","zip","city")

def dfWithDepartement(df):
    substrZip = substring(col("zip"),0,2)
    corseCase = when(col("zip") <= "20190" , "2A").otherwise("2B")
    return df.withColumn("departement",when(substrZip != "20",substrZip).otherwise(corseCase))

def clean(dfClient,dfCity):
    #filtre l'age
    dfClientsFiltered = filterByAge(dfClient)
    #fais la jointure sur le zip et écris le fichier au format parquet
    dfJoin = joinDFByZip(dfClientsFiltered,dfCity)
    #ajoute la colonne departement
    dfDepartement = dfWithDepartement(dfJoin)
    return dfDepartement

def main():
    spark = SparkSession.builder \
        .appName("city") \
        .master("local[*]") \
        .getOrCreate()

    #lecture des fichiers
    dfClients = spark.read.option("header", "true").csv("src/resources/exo2/clients_bdd.csv")
    dfCity = spark.read.option("header", "true").csv("src/resources/exo2/city_zipcode.csv")


    dfDepartement = clean(dfClients,dfCity)
    dfDepartement.write.mode("overwrite").parquet("data/exo2/output")
    
