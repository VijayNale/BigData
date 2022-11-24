from pyspark import SparkContext, StorageLevel

from sys import stdin

sc = SparkContext("local[*]","customer-orders.csv");

input = sc.textFile("C:/Users/Vijay/Desktop/shared/week10/customerorders.csv")

mapped_input = input.map(lambda x: (x.split(",")[0],float(x.split(",")[2])))

total_by_cust =mapped_input.reduceByKey(lambda x,y: x+y)

premium_cust = total_by_cust.filter(lambda x: x[1] > 5000)

double_amt = premium_cust.map(lambda x: (x[0],x[1]*2)).persist(StorageLevel.MEMORY_ONLY)

result = double_amt.collect()

for i in result:
    print(i)

print(double_amt.count())

stdin.readline()