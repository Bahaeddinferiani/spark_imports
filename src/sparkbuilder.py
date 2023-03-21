from pyspark.sql import *
from pyspark import *

def sparkbuilder():
    spark = SparkSession.builder.appName('flask').getOrCreate()
    return spark