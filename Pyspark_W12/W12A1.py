from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

my_conf = SparkConf()
my_conf.set("spark.app.name","order")
my_conf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=my_conf).enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

emp_df = spark.read.format("json")\
    .option("header",True)\
    .option("inferschema",True)\
    .option("path","C:/Users/Vijay/Desktop/shared/week12/employee.json")\
    .load()


dept_df = spark.read.format("json")\
    .option("header",True)\
    .option("inferschema",True)\
    .option("path","C:/Users/Vijay/Desktop/shared/week12/dept.json")\
    .load()

#emp_df.printSchema()
#dept_df.printSchema()
#emp_df.show()
#dept_df.show()

#Join
joinCondition = dept_df.deptid == emp_df.deptid
jointype = "left"

joinDf = dept_df.join(emp_df,joinCondition,jointype)

joinnew_Df = joinDf.drop(emp_df.deptid)

joinnew_Df.show()
Final_Df = joinnew_Df.drop_duplicates(["id","empname"])

# column object expression
Final_Df.groupby("deptid")\
    .agg(count("empname").alias("Emp_Count"),first("deptName").alias("Dept_Name"))\
    .sort(desc("Emp_Count"))\
    .show()

#string expression
Final_Df.groupBy("deptid",)\
    .agg(expr("count(empname) as Emp_Count"),
         expr("first(deptName) as Dept_Name"))\
    .show()


#spark SQL
Final_Df.createOrReplaceTempView("final")
spark.sql("""select deptid,count(empname) as Emp_Count,first(deptName) as Dept_Name from final group by deptid order by Emp_Count desc""").show()



spark.stop()

