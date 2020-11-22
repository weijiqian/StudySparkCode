from pyspark import SparkContext, SparkConf, Row
from pyspark.sql import SparkSession
from datetime import datetime
from testbean import bean

conf = SparkConf().setAppName("").setMaster("")
sc = SparkContext('local', 'test')
spark = SparkSession.builder.master("local").getOrCreate()

def g(x):
    print(x)

l = [('Alice', 1)]

rdd = sc.parallelize(l)
person = rdd.map(lambda r: bean.Person(*r))

df2 = spark.createDataFrame(person)
df2.foreach(g)
df2.select("name").show()

spark.stop()
