from pyspark.sql import SparkSession
spark = SparkSession.builder \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()
data = [('James','Smith','M',3000), ('Anna','Rose','F',4100),
  ('Robert','Williams','M',6200)
]
columns = ["firstname","lastname","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)
df.show()

############################################################################################
from pyspark.sql.functions import *
df.withColumn("grade", when((df.salary < 4000) , lit("A")).when((df.salary >= 4000) & (df.salary <= 5000) , lit("B"))\
              .otherwise(lit("C"))).show()

############################################################################################
df.select(count("*").alias("Total Count"), sum("salary").alias("Total Sum"),avg("salary").alias("Average salary")).show()

############################################################################################
df.createOrReplaceTempView("table")
df1 = spark.sql("select count(*), sum(salary), avg(salary) from table")
df1.show()

############################################################################################
df.groupby("gender").sum("salary").show(truncate=False)

############################################################################################

df.filter(col("gender") == "M").show()
df.where(col("gender") == "F").show()

############################################################################################
"""dens_rank"""
from pyspark.sql.window import Window
partition=Window.partitionBy("gender").orderBy("salary")
df.withColumn("Rank",dense_rank().over(partition)).show()
############################################################################################
#UDF
from pyspark.sql.types import StringType

def upperCase(str):
    return str.upper()

spark.udf.register("parseAgeFunction",upperCase,StringType())
# SQL expression : function that which register in catelog
df.withColumn("Upper Case",expr("parseAgeFunction(firstname)")).show()
