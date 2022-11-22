from pyspark import SparkContext

sc = SparkContext("local[*]","movie-data")

lines = sc.textFile("C:/Users/Vijay/Desktop/shared/week9/moviedata.data")

ratings = lines.map(lambda x: (x.split("\t")[2],1))

result = ratings.reduceByKey(lambda x,y: x+y).collect()

for a in result:
    print(a)