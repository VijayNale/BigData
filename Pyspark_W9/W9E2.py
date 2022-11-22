from pyspark import SparkContext
from sys import stdin

if __name__ == "__main__":
    # comman lines
    sc = SparkContext = SparkContext("local[*]","WorldCount")

    sc.setLogLevel("ERROR")

    input = sc.textFile('C:/Users/Vijay/Desktop/shared/week9/search_data.txt')

    # one input rows will give multiple outputs rows
    words = input.flatMap(lambda x : x.split(" "))

    words_caps = words.map(lambda x: x.lower())

    # one input row will give one output rows
    word_counts = words_caps.map(lambda  x: (x,1))

    final_count = word_counts.reduceByKey(lambda x,y : x+y)

    result = final_count.sortBy(lambda x: x[1], False).collect()

    for a in result:
        print(a)
else:
    print("run it indirectly")

stdin.readline()