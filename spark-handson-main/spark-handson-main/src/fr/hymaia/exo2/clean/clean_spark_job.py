import pyspark.sql.functions as f
from pyspark.sql.functions import split
from pyspark.sql.functions import when
from src.fr.hymaia.exo2.aggregate.aggregate_spark_job import population_counter

def filter_clients(clients_data):
    return clients_data.filter(clients_data.age > 18)

def joint_zip_clients(clients_data,zip_data):
    return clients_data.join(zip_data,clients_data.zip == zip_data.zip, "inner").drop(clients_data.zip)

def Departement_generate(df):
    return df.withColumn("departement",when((df["zip"].substr(1, 2) == 20) & (df["zip"] <= "20190"), "2A").when((df["zip"].substr(1, 2) == 20) & (df["zip"] > "20190"), "2B").otherwise(df["zip"].substr(1, 2)))

def filter_joint_depGen(clients,zip_data):
    return Departement_generate(joint_zip_clients(filter_clients(clients),zip_data))

