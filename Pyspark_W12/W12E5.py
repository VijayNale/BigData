from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType

my_conf = SparkConf()
my_conf.set("spark.app.name","dataset")
my_conf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.format("csv")\
    .option("inferschema",True)\
    .option("path","C:/Users/Vijay/Desktop/shared/week12/dataset.dataset1")\
    .load()

df1= df.toDF("name","age","city")

def ageCheck(age):
    if(age > 18):
        return "Y"
    else:
        return "N"

spark.udf.register("parseAgeFunction",ageCheck,StringType())

for x in spark.catalog.listFunctions():
    print(x)

# SQL expression : function that which register in catelog
df2 = df1.withColumn("Adult",expr("parseAgeFunction(age)"))

"""
# Column object expression 
parseAgeFunction = udf(ageCheck,StringType())

df2 = df1.withColumn("Adult",parseAgeFunction("age"))
"""

df2.printSchema()
df2.show()


spark.stop()