from pyspark import SparkContext
sc = SparkContext(appName="fre_item")
from operator import add
import sys
def createC1(dataset):
	"Create a list of candidate item sets of size one."
	c1 = []
	for transaction in dataset:
		for item in transaction:
			if not [item] in c1:
				c1.append([item])
	c1.sort()
	#frozenset because it will be a ket of a dictionary.
	return map(frozenset, c1)
 
 
def scanD(dataset, candidates, min_support):
	"Returns all candidates that meets a minimum support level"
	sscnt = {}
	for tid in dataset:
		for can in candidates:
			if can.issubset(tid):
				sscnt.setdefault(can, 0)
				sscnt[can] += 1
 
	retlist = []
	support_data = {}
	for key in sscnt:
		support = sscnt[key] 
		if support >= min_support:
			retlist.insert(0, key)
		support_data[key] = support
	return retlist, support_data
 
 
def aprioriGen(freq_sets, k):
	"Generate the joint transactions from candidate sets"
	retList = []
	lenLk = len(freq_sets)
	for i in range(lenLk):
		for j in range(i + 1, lenLk):
			L1 = list(freq_sets[i])[:k - 2]
			L2 = list(freq_sets[j])[:k - 2]
			L1.sort()
			L2.sort()
			if L1 == L2:
				retList.append(freq_sets[i] | freq_sets[j])
	return retList

def transform_function(rdd):
	dataset= rdd.collect()
	rdd1_c1 = createC1(dataset)
	dataset = map(set,dataset)
	f,s = scanD(dataset,rdd1_c1,6)
	dual_1 = aprioriGen(f,2)
	L2,s = scanD(dataset,dual_1,6)
	a=[]
	for i in dataset:
		for j in L2:
				if j.issubset(i):
						a.append([j,1])
	a = sc.parallelize(a)
	a = a.reduceByKey(add)
	return a
if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: fre_tem_pyspark <input_file>"
        exit(-1)
	
data = sc.textFile(sys.argv[1])
data1 = data.map(lambda line : ([int(x) for x in line.split(" ")]))
rdd1,rdd2 = data1.randomSplit([10,10],0)
trasnformed_rdd1 = transform_function (rdd1)
trasnformed_rdd2 = transform_function (rdd2)
total = trasnformed_rdd2.join(trasnformed_rdd1)
final = total.map(lambda (k,(v,w)):(k,v+w))
result = final.filter(lambda (k,v):v >=12)
print result.collect()
