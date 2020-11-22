from pyspark import SparkContext, SparkConf, Row
from pyspark.sql import SparkSession
from datetime import datetime

conf = SparkConf().setAppName("").setMaster("")
spark = SparkSession.builder.master("local").getOrCreate()

def g(x):
    print(x)

l = [('Alice', 1)]

spark.createDataFrame(l, ['name', 'age']).show()

spark.stop()
