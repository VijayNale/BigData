from pyspark import SparkContext

sc = SparkContext("local[*]","customer-orders")

rdd1 = sc.textFile("C:/Users/Vijay/Desktop/shared/week9/customerorders.csv")

rdd2 = rdd1.map(lambda x: (x.split(",")[0],float(x.split(",")[2])))

rdd3 = rdd2.reduceByKey(lambda x,y: x+y)

rdd4 = rdd3.sortBy(lambda x: x[1],False).collect()

for a in rdd4:
    print(a)