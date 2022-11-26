from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType,StringType
from pyspark.sql.functions import *

my_conf = SparkConf()
my_conf.set("spark.app.name","order")
my_conf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

bat_Hist_rdd = spark.sparkContext.textFile("C:/Users/Vijay/Desktop/shared/week12/FileA_BatsmenDetails_History.txt")
bat_His_rdd1 = bat_Hist_rdd.map(lambda x: (x.split("\t")[0],x.split("\t")[1],x.split("\t")[2],x.split("\t")[3],x.split("\t")[4]))
columns = ["MatchNumber","Batsman","Team","RunsScored","StrikeRat"]
bat_His_df = spark.createDataFrame(bat_His_rdd1,schema=columns)
#bat_His_df.printSchema()
#bat_His_df.show()

bat_Worldcup_rdd = spark.sparkContext.textFile("C:/Users/Vijay/Desktop/shared/week12/FileB_BatsemenDetails_Worldcup2019.txt")
bat_Worldcup_rdd1 = bat_Worldcup_rdd.map(lambda x: (x.split("\t")[0],x.split("\t")[1]))
columns_Worldcup = ["batsman","team"]
bat_Worldcup_df = spark.createDataFrame(bat_Worldcup_rdd1,schema=columns_Worldcup)
#bat_Worldcup_df.printSchema()
#bat_Worldcup_df.show()


batsmenBestRunsAvgHistoryD = bat_His_df.groupby("batsman")\
    .agg(avg("RunsScored").alias("AverageRunsScored"))\
    .select("batsman","AverageRunsScored")

#batsmenBestRunsAvgHistoryD.show()
#bat_Worldcup_df.show()

spark.sql("SET spark.sql.autoBroadcastJoinThreshold =-1")

joinCondition = batsmenBestRunsAvgHistoryD.batsman == bat_Worldcup_df.batsman

joinType="inner"

finalBestBatsmenPlayingWorldCupDf = batsmenBestRunsAvgHistoryD.\
                           join(broadcast(bat_Worldcup_df),joinCondition,joinType)\
                           .drop(batsmenBestRunsAvgHistoryD.batsman)\
                           .sort(desc("AverageRunsScored"))

finalBestBatsmenPlayingWorldCupDf.drop("team").show()

