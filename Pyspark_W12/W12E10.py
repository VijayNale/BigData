# Window Aggregations

from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *

my_conf = SparkConf()
my_conf.set("spark.app.name", "my first application")
my_conf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

invoiceDF = spark.read\
    .format("csv")\
    .option("inferSchema", True) \
    .option("path", "C:/Users/Vijay/Desktop/shared/week12/windowdata.csv") \
    .load()

invoiceDF = invoiceDF.selectExpr("_c0 as country","_c1 as weeknum","_c2 as numinvoices","_c3 as totalquantity","_c4 as invoicevalue")

invoiceDF.show()

myWindow = Window.partitionBy("country")\
    .orderBy("weeknum")\
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

mydf = invoiceDF.withColumn("RunningTotal",sum("invoicevalue").over(myWindow))

mydf.show()