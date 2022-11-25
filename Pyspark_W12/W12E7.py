# simple aggregation

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


my_conf = SparkConf()
my_conf.set("spark.app.name","my first application")
my_conf.set("spark.master","local[2]")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

invoiceDf = spark.read.format("csv")\
    .option("header",True)\
    .option("inferSchema",True)\
    .option("path","C:/Users/Vijay/Desktop/shared/week12/order_data.csv")\
    .load()
#invoiceDf.show()

# column object expression
invoiceDf.select(
    count("*").alias("RowCount"),
    sum("Quantity").alias("TotalQuanity"),
    avg("UnitPrice").alias("AvgPrice"),
    countDistinct("InvoiceNo").alias("CountDistinct")).show()

# column string expression
invoiceDf.selectExpr(
    "count(*) as RowCount",
    "sum(Quantity) as TotalQuanity",
    "avg(UnitPrice) as AvgPrice",
    "count(Distinct(InvoiceNo)) as CountDistinct").show()

# spark SQl
invoiceDf.createOrReplaceTempView("Sales")
spark.sql("select count(*) as RowCount, sum(Quantity) as TotalQuanity ,avg(UnitPrice) as AvgPrice ,count(Distinct(InvoiceNo)) as CountDistinct from Sales").show()



spark.stop()