from pyspark import SparkContext, SparkConf, Row
from pyspark.sql import SparkSession
from pyspark.sql.types import *

conf = SparkConf().setAppName("").setMaster("")
sc = SparkContext('local', 'test')
spark = SparkSession.builder.master("local").getOrCreate()

l = [('Alice', 1)]
rdd = sc.parallelize(l)

def g(x):
    print(x)

schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)])
df3 = spark.createDataFrame(rdd, schema)
df3.toDS
df3.foreach(g)


spark.stop()
