from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, to_date, col,to_timestamp, from_unixtime, unix_timestamp

def to_date_(col, formats=("MM/dd/yyyy HH:mm", "dd-MM-yyyy HH:mm:ss")):
    return coalesce(*[to_timestamp(col, f) for f in formats])

my_conf = SparkConf()
my_conf.set("spark.app.name","Project_Data_Cleaning")
my_conf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

card_tran = spark.read.format("csv")\
    .option("path","C:/Users/Vijay/Desktop/shared/project/card_transactions_duplicate.csv")\
    .load()

#card_tran.printSchema()
#card_tran.show()

columns = ["card_id","member_id","amount","postcode","pos_id","transaction_dt","Status"]
card_trans = spark.createDataFrame(card_tran.rdd,schema=columns).repartition(4)

#card_trans.printSchema()
#card_trans.show()

print("Distinct count : "+str(card_trans.count()))
drop_duplic = card_trans.dropDuplicates(["card_id","transaction_dt"])
print("Distinct count of card_id & transaction_dt : "+str(drop_duplic.count()))
#drop_duplic.show()

drop_duplic = drop_duplic.withColumn("transaction_date", to_date_("transaction_dt"))

drop_duplic = drop_duplic.drop(col("transaction_dt"))

drop_duplic.show()

final_df = drop_duplic.select("card_id","member_id","amount","postcode","pos_id","transaction_date","Status")
final_df.show()

final_df = final_df.repartition(1)

final_df.write.format("csv")\
    .option("header",True)\
    .mode("overwrite")\
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")\
    .option("path","C:/Users/Vijay/Desktop/shared/project/CleanResult")\
    .save()

spark.stop()
