from pyspark import SparkConf
from pyspark.sql import SparkSession

myconf = SparkConf()
myconf.set("spark.app.name","my first application")
myconf.set("spark.master","local[2]")

spark = SparkSession.builder.config(conf = myconf).getOrCreate()

order_df = spark.read.option("header",True).option("inferSchema",True).csv("C:/Users/Vijay/Desktop/shared/week11/orders.csv")
#order_df.printSchema()

grouped_df = order_df.repartition(4) \
    .where("order_customer_id > 10000") \
    .select("order_id","order_customer_id") \
    .groupBy("order_customer_id") \
    .count()

grouped_df.show(20)

spark.stop()


