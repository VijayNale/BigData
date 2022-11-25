from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

my_conf = SparkConf()
my_conf.set("spark.app.name","Assigment1")
my_conf.set("spark.master","local[2]")
my_conf.set("spark.jars","D:\spark_avro_2.11-2.4.4.jar")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

#Explicit Schema :- Struct Type
window_schema = StructType([
    StructField("Country",StringType()),
    StructField("weeknum",IntegerType()),
    StructField("numinvoices",IntegerType()),
    StructField("totalquantity",IntegerType()),
    StructField("invoicevalue",DoubleType())
])


window_df = spark.read.format("csv")\
    .schema(window_schema)\
    .option("path","C:/Users/Vijay/Desktop/shared/week11/windowdata.csv")\
    .load()

window_df.printSchema()
window_df.show()
"""
window_df.write.option("header",True)\
    .partitionBy("Country","weeknum")\
    .mode("overwrite")\
    .option("path","C:/Users/Vijay/Desktop/shared/week11/parquestresult")\
    .save()

"""
window_df.write.format("avro")\
    .partitionBy("Country")\
    .mode("overwrite")\
    .option("path","C:/Users/Vijay/Desktop/shared/week11/ArvoResult")\
    .save()
