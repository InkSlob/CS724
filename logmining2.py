from pyspark import SparkContext
import sys
if len(sys.argv) != 2:
	print >> sys.stderr, "Usage: logmining2 <file>"
	exit(-1)
sc = SparkContext(appName="logmining2")
tf = sc.textFile(sys.argv[1])
search_keywords = ["error", "Mozilla", "compatible", "iPhone"]
def is_found(line):
	return any(keyword in line for keyword in search_keywords)
found = tf.filter(is_found)
count = found.count()
print "The total keywords found %d", count
#found.saveAsTextFile(sys.argv[2])
sc.stop()
