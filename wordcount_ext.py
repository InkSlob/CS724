import sys
from operator import add
from pyspark import SparkContext


if len(sys.argv) != 3:
    print >> sys.stderr, "Usage: wordcount <input_file> <output_file>"
    exit(-1)
sc = SparkContext(appName="PythonWordCount")
lines = sc.textFile(sys.argv[1])\
    .map( lambda x: x.replace(',',' ').replace('.',' ').replace('-',' ').lower()) \
    .flatMap(lambda x: x.split()) \
    .map(lambda x: (x, 1)) \
    .reduceByKey(lambda x,y:x+y) \
    .map(lambda x:(x[1],x[0])) \
    .sortByKey(False) 

print lines.take(10)






#lines_low = lines.map(lambda line: line.lower())
#counts = lines_low.flatMap(lambda line: line.strip().split(' ')).map(lambda word: (word, 1)).reduceByKey(add)
#ordered = counts.takeOrdered(10, key=lambda x: -x[1])
#lines.saveAsTextFile(sys.argv[2])

sc.stop()
