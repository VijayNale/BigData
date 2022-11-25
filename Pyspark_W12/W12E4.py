from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

my_conf = SparkConf()
my_conf.set("spark.app.name","order")
my_conf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

order_df = spark.read.format("csv")\
    .option("header",True)\
    .option("inferschema",True)\
    .option("path","C:/Users/Vijay/Desktop/shared/week12/orders.csv")\
    .load()

#order_df.printSchema()
#order_df.show(5)

order_df.select("order_id","order_date").show()
order_df.select(col("order_id")).show()
order_df.select(column("order_date")).show()

spark.stop()

