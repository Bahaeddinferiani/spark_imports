from pyspark.sql import *

def sparkbuilder():
    spark = SparkSession.builder.master('local').appName('new').getOrCreate()
    return spark