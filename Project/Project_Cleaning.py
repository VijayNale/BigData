from pyspark import SparkConf
from pyspark.sql import SparkSession

my_conf = SparkConf()
my_conf.set("spark.app.name","ProjectCleaning")
my_conf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

card_tran = spark.read.format("csv")\
    .option("path","C:/Users/Vijay/Desktop/shared/project/card_transactions.csv")\
    .load()

#card_tran.printSchema()
#card_tran.show()

columns = ["card_id","member_id","amount","postcode","pos_id","transaction_dt","Status"]
card_trans = spark.createDataFrame(card_tran.rdd,schema=columns).repartition(4)

card_trans.printSchema()
card_trans.show()

print("Distinct count : "+str(card_trans.count()))
drop_duplic = card_trans.dropDuplicates(["card_id","transaction_dt"])
print("Distinct count of card_id & transaction_dt : "+str(drop_duplic.count()))
drop_duplic.show()

drop_duplic = drop_duplic.repartition(1)

drop_duplic.write.format("csv")\
    .option("header",True)\
    .mode("overwrite")\
    .option("path","C:/Users/Vijay/Desktop/shared/project/CleanResult")\
    .save()

spark.stop()
