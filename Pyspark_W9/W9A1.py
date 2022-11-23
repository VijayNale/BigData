from pyspark import SparkContext

def parseLine(line):
    fields = line.split(",")
    age = int(fields[1]);
    if (age > 18):
        return (fields[0],fields[1],fields[2],'Y')
    else:
        return (fields[0], fields[1], fields[2], 'N')

sc = SparkContext("local[*]","age-problem")

rdd = sc.textFile('C:/Users/Vijay/Desktop/shared/week9/dataset.dataset1')

parsed_result = rdd.map(parseLine)

result = parsed_result.collect()

for i in result:
    print(i)

