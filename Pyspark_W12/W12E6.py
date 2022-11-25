from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


my_conf = SparkConf()
my_conf.set("spark.app.name","my first application")
my_conf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

my_list = [(1,"2013-07-25",11599,"CLOSED"),
    (2,"2013-07-25",256,"PENDING_PAYMENT"),
    (3,"2013-07-25",11599,"COMPLETE"),
    (4,"2013-07-25",8827,"CLOSED")]

orderDf = spark.createDataFrame(my_list)\
    .toDF("orderid","orderdate","customerid","status")


newDf = orderDf\
    .withColumn("date1",unix_timestamp(col("orderdate")))\
    .withColumn("newid",monotonically_increasing_id())\
    .drop_duplicates(["orderdate","customerid"])\
    .drop("orderid")\
    .sort("orderdate")

orderDf.printSchema()
orderDf.show()
newDf.show()

spark.stop()
