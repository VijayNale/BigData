from pyspark import SparkContext

sc = SparkContext("local[*]","bigLog.txt")

sc.setLogLevel("ERROR")

base_rdd = sc.textFile("C:/Users/Vijay/Desktop/shared/week10/bigLog.txt")

#mapped_rdd = base_rdd.map(lambda x: (x.split(":")[0],x.split(":")[1]))\
mapped_rdd = base_rdd.map(lambda x: (x.split(":")[0],1))

#grouped_rdd = mapped_rdd.groupByKey()
reduced_rdd = mapped_rdd.reduceByKey(lambda x,y:x+y)

#result = grouped_rdd.collect()
result = reduced_rdd.collect()

for i in result:
    print(i)



