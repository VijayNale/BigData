from pyspark import SparkContext

def blanklinechecker(line):
    if (len(line) == 0):
        my_accum.add(1)

sc = SparkContext("local[*]","sample-txt-find-blank-line")

myrdd = sc.textFile("C:/Users/Vijay/Desktop/shared/week10/samplefile.txt")

#interger accumlator
my_accum = sc.accumulator(0)

myrdd.foreach(blanklinechecker)

print(my_accum.value)