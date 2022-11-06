import pyspark.sql.functions as f


def population_counter(Departement_data):
    return Departement_data.groupBy("departement").count().withColumnRenamed("count", "np_people")
