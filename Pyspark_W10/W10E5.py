"""
from pyspark import SparkContext

sc = SparkContext("local[*]","customer-order")

sc.setLogLevel("ERROR")

input = sc.textFile("C:/Users/Vijay/Desktop/shared/week10/customerorders.csv")

#input.saveAsTextFile("C:/Users/Vijay/Desktop/shared/week10/output")

print("Count",input.count())
print("getNumPartitions",input.getNumPartitions())
print("defaultParallelism",sc.defaultParallelism)
print("defaultMinPartitions",sc.defaultMinPartitions)

new_rdd = input.repartition(10)

print("getNumPartitions",new_rdd.getNumPartitions())
print("defaultParallelism",sc.defaultParallelism)
print("defaultMinPartitions",sc.defaultMinPartitions)


new_latest = input.coalesce(1)

print("getNumPartitions",new_latest.getNumPartitions())
print("defaultParallelism",sc.defaultParallelism)
print("defaultMinPartitions",sc.defaultMinPartitions)


#------------------------------------------------------

## parallelize variable
from pyspark import SparkContext

sc = SparkContext("local[*]","pyspark")

a = range(1,101)
base = sc.parallelize(a)
result = base.reduce(lambda x,y: x+y)
print(result)
"""