from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract

my_conf = SparkConf()
my_conf.set("spark.app.name","order")
my_conf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

myregx = r'^(\S+) (\S+)\t(\S+)\,(\S+)'

line_df = spark.read.text("C:/Users/Vijay/Desktop/shared/week12/orders_new.csv")
#line_df.printSchema()
#line_df.show()

final_df = line_df.select(regexp_extract('value',myregx,1).alias("order_id"),
               regexp_extract('value',myregx,2).alias("Date"),
               regexp_extract('value',myregx,3).alias("customer_id"),
               regexp_extract('value',myregx,4).alias("status"))

final_df.printSchema()
final_df.show()

final_df.select("order_id").show()

final_df.groupby("status").count().show()