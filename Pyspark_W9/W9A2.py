from pyspark import SparkContext

def parseLine(line):
    fields = line.split(",")
    station_id = fields[0];
    entry_type = fields[2];
    temp = float(fields[3]);
    return (station_id, entry_type, temp)

sc = SparkContext("local[*]","tempdata")

sc.setLogLevel("ERROR")

rdd = sc.textFile('C:/Users/Vijay/Desktop/shared/week9/tempdata.csv')

parsed_result = rdd.map(parseLine)

rdd3 = parsed_result.filter(lambda x: x[1] == 'TMIN')

rdd4 = rdd3.map(lambda x: (x[0],x[2]))

rdd5 = rdd4.reduceByKey(lambda x,y: min(x,y))

result = rdd5.collect()

for i in result:
    print('{} Station minimum temperature : {}'.format(i[0], i[1]))


