from pyspark import SparkContext

# comman lines
sc = SparkContext = SparkContext("local[*]","WorldCount")

sc.setLogLevel("ERROR")

input = sc.textFile('C:/Users/Vijay/Desktop/shared/week9/search_data.txt')

# one input rows will give multiple outputs rows
words = input.flatMap(lambda x : x.split(" "))

words_caps = words.map(lambda x: x.upper())

# one input row will give one output rows
word_counts = words_caps.map(lambda  x: (x,1))

final_count = word_counts.reduceByKey(lambda x,y : x+y)

result = final_count.collect()

print(sorted(result,key= lambda x: x[1], reverse=True))

#for a in result:
#    print(a)
