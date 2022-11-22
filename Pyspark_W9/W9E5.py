def parseLine(line):
    fields = line.split("::")
    age = int(fields[2]);
    numFriends = int(fields[3])
    return (age,numFriends)

from pyspark import SparkContext

sc = SparkContext("local[*]","friendsdata")

lines = sc.textFile("C:/Users/Vijay/Desktop/shared/week9/friendsdata.csv")

rdd = lines.map(parseLine)

#print(rdd.collect())

totoalByAge = rdd.mapValues(lambda x: (x,1)).reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))

#print(totoalByAge.collect())

avarageByAge = totoalByAge.mapValues(lambda x: x[0]/x[1])

result = avarageByAge.collect()

for i in result:
    print(i)
