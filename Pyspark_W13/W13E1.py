import random
from pyspark import SparkConf
from pyspark.sql import SparkSession

my_conf = SparkConf()
my_conf.set("spark.app.name","order")
my_conf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

def randomGenerator():
    return random.randint(1,60)

def myFunction(x):
    if(x[0][0:4]=="WARN"):
        return("WARN",x[1])
    else:
        return ("ERROR",x[1])

rdd1 = spark.sparkContext.textFile("C:/Users/Vijay/Desktop/shared/week13/bigLogNew.txt")

rdd2 = rdd1.map(lambda x:(x.split(":")[0] + str(randomGenerator()),x.split(":")[1]))

rdd3 = rdd2.groupByKey()

rdd4 = rdd3.map(lambda x: (x[0] ,len(x[1])))

rdd4.cache()

rdd5 = rdd4.map(lambda x: myFunction(x))

rdd6 = rdd5.reduceByKey(lambda x,y: x+y)

result = rdd6.collect()

for i in result:
    print(i)

import sys
sys.stdin.readline()

spark.stop()
