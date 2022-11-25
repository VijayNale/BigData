# simple aggregation

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


my_conf = SparkConf()
my_conf.set("spark.app.name","my first application")
my_conf.set("spark.master","local[2]")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

orderDf = spark.read\
    .format("csv")\
    .option("header",True)\
    .option("inferSchema",True)\
    .option("path","C:/Users/Vijay/Desktop/shared/wek12/orders.csv")\
    .load()

customerDf = spark.read\
    .format("csv")\
    .option("header",True)\
    .option("inferSchema",True)\
    .option("path","C:/Users/Vijay/Desktop/shared/wek12/customers.csv")\
    .load()

#orderDf.show()
#customerDf.show()

joinCondition = orderDf.order_customer_id == customerDf.customer_id

joinDf = orderDf.join(customerDf,joinCondition,"inner")

joinDf.show()

spark.stop()
