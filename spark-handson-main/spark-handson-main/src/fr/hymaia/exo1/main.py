import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from pyspark.sql.functions import when

def main():
    pass

def wordcount(df, col_name):
    return df.withColumn('word', f.explode(f.split(f.col(col_name), ' '))) \
        .groupBy('word') \
        .count()
