from pyspark import SparkConf
from pyspark.sql import SparkSession

my_conf = SparkConf()
my_conf.set("spark.app.name","order")
my_conf.set("spark.master","local[*]")
my_conf.set("spark.jars","D:\spark_avro_2.11-2.4.4.jar")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

order_df = spark.read.format("csv")\
    .option("header",True)\
    .option("inferschema",True)\
    .option("path","C:/Users/Vijay/Desktop/shared/week12/orders.csv")\
    .load()

#print("No of pariton are :",order_df.rdd.getNumPartitions())
#orderRep = order_df.repartition(4)

order_df.write.format("avro")\
    .mode("overwrite")\
    .option("path","C:/Users/Vijay/Desktop/shared/week12/OrderAvro")\
    .save()

#SaveMode:- overwrite,ignore,append,errorifexists
