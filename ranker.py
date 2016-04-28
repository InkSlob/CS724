import re
import sys
from operator import add
from pyspark import SparkContext

def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)

sc = SparkContext(appName="PythonPageRank")

def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)  #s+ for whitespace
    return parts[0], parts[1]

# change number to be one more than number of command line arguments
if len(sys.argv) != 6:
        print >> sys.stderr, "Usage: pagerank.py <input_file> <input_file2> <number of iterations>"
        exit(-1)

# pulls in hollins.txt
lines = sc.textFile(sys.argv[1], 1)

links = (lines
         .map(lambda urls: parseNeighbors(urls))
         .distinct()
         .groupByKey()
         .cache())

ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

# removed printing from original to clean up output

# This is how many iterations there are (20)
for iteration in xrange(int(sys.argv[5])):
	print ("\n*** Iteration %d***" %iteration)
	contribs = links.join(ranks).flatMap(lambda (url, (urls, rank)): computeContribs(urls, rank))
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

# pulls in hollins_info.txt
data = sc.textFile(sys.argv[2], 1)

# convert to key value pairs and reverse order
parsed = (data
          .map(lambda x:x.strip().split(" "))
          .map(lambda (key,val):(int(key),val)))


# removed ranks printing from original to reduce output

# find the min and max of the ranks rdd by switching the key value pairs
max = ranks.map(lambda (key,val):(val,key)).max()
min = ranks.map(lambda (key,val):(val,key)).min()

# use parsed rdd and max & min rdds to join ranks with urls, filter the max and min
# takes top and bottom single entries
max_page = parsed.filter(lambda (id,url):id == int(max[1]))
min_page = parsed.filter(lambda (id,url):id == int(min[1]))

# couldn't get it to print so just show rdds, saved outputs to textfiles and included as part of submission.
print max_page
print min_page

max_page.repartition(1).saveAsTextFile(sys.argv[3])
min_page.repartition(1).saveAsTextFile(sys.argv[4])


sc.stop()
