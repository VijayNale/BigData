from pyspark import SparkContext

sc = SparkContext("local[*]","loglevelCount")

sc.setLogLevel("ERROR")

if __name__ == "__main__":
    my_list = ["ERROR: Thu Jun 04 10:37:51 BST 2015",
    "WARN: Sun Nov 06 10:37:51 GMT 2016",
    "WARN: Mon Aug 29 10:37:51 BST 2016",
    "ERROR: Thu Dec 10 10:37:51 GMT 2015",
    "ERROR: Fri Dec 26 10:37:51 GMT 2014",
    "ERROR: Thu Feb 02 10:37:51 GMT 2017",
    "WARN: Fri Oct 17 10:37:51 BST 2014",
    "ERROR: Wed Jul 01 10:37:51 BST 2015"]

    original_logs_rdd = sc.parallelize(my_list)
else:
    original_logs_rdd = sc.textFile("C:/Users/Vijay/Desktop/shared/week10/bigLog.txt")
    print("else Part we are..")

rdd1 = original_logs_rdd.map(lambda x: (x.split(":")[0].lower(),1))

rdd2 = rdd1.reduceByKey(lambda x,y:x+y)

result = rdd2.collect()

for x in result:
    print(x)