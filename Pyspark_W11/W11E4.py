from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, StringType

myconf = SparkConf()
myconf.set("spark.app.name","my first application")
myconf.set("spark.master","local[2]")

#Explicit Schema :- Struct Type
order_schema = StructType([
    StructField("orderid",IntegerType()),
    StructField("orderdate",TimestampType()),
    StructField("customerid",IntegerType()),
    StructField("status",StringType())
])

#Explicit Schema :- DDL String
order_DDL = """orderidnew Integer, orderdate Timestamp,
               customerid Integer,status String"""

spark = SparkSession.builder.config(conf = myconf).getOrCreate()

order_df = spark.read.format("csv")\
    .option("header",True)\
    .schema(order_DDL)\
    .option("path","C:/Users/Vijay/Desktop/shared/week11/orders.csv")\
    .load()

order_df.printSchema()
#.option("inferSchema",True)\

order_df.show(20)

spark.stop()


