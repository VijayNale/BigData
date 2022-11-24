from pyspark import SparkContext

sc = SparkContext("local[*]","join_demo");

sc.setLogLevel("ERROR")

ratings_rdd = sc.textFile("C:/Users/Vijay/Desktop/shared/week11/ratings.dat")

mapped_rdd = ratings_rdd.map(lambda x: (x.split("::")[1],x.split("::")[2]))

new_mapped_rdd = mapped_rdd.mapValues(lambda x: (float(x),1.0))

reduce_rdd = new_mapped_rdd.reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))

filterd_rdd = reduce_rdd.filter(lambda x: x[1][1] > 1000)
#print(filterd_rdd.collect())

final_rdd = filterd_rdd.mapValues(lambda x: x[0]/x[1]).filter(lambda x: x[1] > 4.5)
#print(final_rdd.collect())

movies_rdd = sc.textFile("C:/Users/Vijay/Desktop/shared/week11/movies.dat")

movies_mapped_rdd= movies_rdd.map(lambda x: (x.split("::")[0],x.split("::")[1],x.split("::")[2]))

joined_rdd = movies_mapped_rdd.join(final_rdd)

#print(joined_rdd.collect())

top_movies_rdd = joined_rdd.map(lambda x: x[1][0])

result = top_movies_rdd.collect()

for i in result:
    print(i)