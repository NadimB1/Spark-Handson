import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from src.fr.hymaia.exo2.clean.clean_spark_job import *
from src.fr.hymaia.exo2.aggregate.aggregate_spark_job import *

def main():
    spark = SparkSession.builder.appName( "first job" ).master( "local[*]").getOrCreate()
    zip_data = spark.read.csv("/home/nadim/spark-handson-main/spark-handson-main/src/resources/exo2/city_zipcode.csv", header=True)
    data = spark.read.csv("/home/nadim/spark-handson-main/spark-handson-main/src/resources/exo1/data.csv", header=True)
    clients_data = spark.read.csv("/home/nadim/spark-handson-main/spark-handson-main/src/resources/exo2/clients_bdd.csv", header=True)


    #clients_zip_join_data = joint_zip_clients(filter_clients(clients_data),zip_data)
    #clients_zip_join_data.write.parquet("/home/nadim/spark-handson-main/src/data/exo2/output.parquet")

# Read a parquet

    parq = spark.read.parquet("/home/nadim/spark-handson-main/spark-handson-main/src/data/exo2/output.parquet")
    parq.createOrReplaceTempView("ParquetTable")
    parq.printSchema()
    parq.show()
    
    Departement_data = Departement_generate(zip_data)
    population_data = population_counter(Departement_data)
    population_data.write.csv("/home/nadim/spark-handson-main/spark-handson-main/src/data/exo2/aggregate")
    #filter_clients(clients_data)
    #filter_joint_depGen(clients_data,zip_data)
    
    
    
