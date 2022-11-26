from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType,StringType
from pyspark.sql.functions import *

my_conf = SparkConf()
my_conf.set("spark.app.name","order")
my_conf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=my_conf).enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

movie_rdd = spark.sparkContext.textFile("C:/Users/Vijay/Desktop/shared/week12/movies.dat")
movie_rdd1 = movie_rdd.map(lambda x: (x.split("::")[0],x.split("::")[1],x.split("::")[2]))
columns = ["movieid","moviename","genre"]
movie_df = spark.createDataFrame(movie_rdd1,schema=columns)
movie_df.printSchema()
movie_df.show()

ratings_rdd = spark.sparkContext.textFile("C:/Users/Vijay/Desktop/shared/week12/ratings.dat")
ratings_rdd1 = ratings_rdd.map(lambda x: (x.split("::")[0],x.split("::")[1],x.split("::")[2],x.split("::")[3]))
columns_ratings = ["userid","movieid","rating","timestamp"]
ratings_df = spark.createDataFrame(ratings_rdd1,schema=columns_ratings)
ratings_df.printSchema()
ratings_df.show()


transformed_Df = ratings_df.groupby("movieid")\
    .agg(count("rating").alias("MovieViewCount"),
         avg("rating").alias("AvgMovieRatings")) \
    .sort(desc("MovieViewCount"))

popularMoviesDf = transformed_Df.filter("MovieViewCount > 1000 AND AvgMovieRatings > 4.5")\

spark.sql("SET spark.sql.autoBroadcastJoinThreshold =-1")

joinCondition = popularMoviesDf.movieid == movie_df.movieid

joinType="inner"

finalPopularMoviesDf = popularMoviesDf.\
                           join(broadcast(movie_df),joinCondition,joinType)\
                           .drop(popularMoviesDf.movieid)\
                           .sort(desc("AvgMovieRatings"))

finalPopularMoviesDf.drop("MovieViewCount","movieid","AvgMovieRatings","genre").show()

spark.stop()
