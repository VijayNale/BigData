from pyspark import SparkContext

def loadboringwords():
    boring_words = set(line.strip() for line in open("C:/Users/Vijay/Desktop/shared/week10/boringwords.txt"))
    return boring_words

sc = SparkContext("local[*]","bigdatacampaigndata")

named_set = sc.broadcast(loadboringwords())

initail_rdd = sc.textFile("C:/Users/Vijay/Desktop/shared/week10/bigdatacampaigndata.csv")

mapped_input = initail_rdd.map(lambda  x: (float(x.split(",")[10]),x.split(",")[0]))

words_rdd = mapped_input.flatMapValues(lambda x: x.split(" "))

final_mapped = words_rdd.map(lambda x: (x[1].lower(),x[0]))

filted_rdd = final_mapped.filter(lambda x: x[0] not in named_set.value)

total = filted_rdd.reduceByKey(lambda x,y:x+y)

#Top 20
sorted = total.sortBy(lambda x:x[1],False)

result = sorted.take(20)

for x in result:
    print(x)
