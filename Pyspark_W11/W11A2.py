from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

my_conf = SparkConf()
my_conf.set("spark.app.name","Assigment1")
my_conf.set("spark.master","local[2]")

#Explicit Schema :- Struct Type
window_schema = StructType([
    StructField("Country",StringType()),
    StructField("weeknum",IntegerType()),
    StructField("numinvoices",IntegerType()),
    StructField("totalquantity",IntegerType()),
    StructField("invoicevalue",DoubleType())
])

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

window_rdd = spark.sparkContext.textFile("C:/Users/Vijay/Desktop/shared/week11/windowdata.csv").map(lambda row:row.split(","))

print(type(window_rdd))
print(window_rdd.collect())

columns = ["Country","weeknum","numinvoices","totalquantity","invoicevalue"]
#window_df = spark.createDataFrame(window_rdd).toDF(*columns)
window_df = spark.createDataFrame(window_rdd,schema=columns).repartition(8)#.toDF(*columns)

#window_df = window_df.selectExpr("_1 as Country","_2 as weeknum","_3 as numinvoices","_4 as totalquantity","_5 as invoicevalue")

window_df.show()
window_df.printSchema()

window_df.write.format("json")\
    .option("header",True)\
    .mode("overwrite")\
    .option("path","C:/Users/Vijay/Desktop/shared/week11/JSONresult")\
    .save()
