from pyspark import SparkConf
from pyspark.sql import SparkSession

my_conf = SparkConf()
my_conf.set("spark.app.name","order")
my_conf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=my_conf).enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

order_df = spark.read.format("csv")\
    .option("header",True)\
    .option("inferschema",True)\
    .option("path","C:/Users/Vijay/Desktop/shared/week12/orders.csv")\
    .load()

"""
order_df.createOrReplaceTempView("orders")
result_df = spark.sql("select order_status,count(*) as Total_Orders from orders group by order_status")
result_df = spark.sql("select order_customer_id,count(*) as Total_Orders from orders where order_status = 'CLOSED' group by order_customer_id order by Total_Orders desc")
result_df.show(5)
"""

spark.sql("create database if not exists retail_db2")
order_df.write.format("csv")\
    .mode("overwrite")\
    .bucketBy(4,"order_customer_id")\
    .sortBy("order_customer_id")\
    .saveAsTable("retail_db2.orders44")

