#load data - text file
#from test_helper import Test

from pyspark import SparkContext
import sys

sc = SparkContext()


#import os.path
#data = os.path.join('triangle_data.txt')
file = sc.textFile(sys.argv[1],1)
#tempFile = NamedTemporaryFile(delete=True) 
#tempFile.close() 

def f1(lines):
    p1=[]
    n1=[]
    n1 = str(lines).split()
    vertex = int(n1[0])
    nodes = n1[1].split(',')
    nodes = [int(j) for j in nodes]
    for i in range(len(nodes)-1):
        for j in range(i+1, len(nodes)):
            x1 = (nodes[i],nodes[j])
            x2 = (x1,[vertex])
            p1.append(x2)
    for i in range(len(nodes)):
        x1 = (vertex, nodes[i])
        x2 = (x1, [-1])
        p1.append(x2)
    return p1

def f2(d_in):
    d_out=[]
    dble=d_in[0]
    near=d_in[1]
    ver1=dble[0]
    ver2=dble[1]
    if -1 in near:
        for i in range(len(near)):
            if(near[i]!=-1 and near[i]<ver1 and near[i]<ver2):
                d_out.append((near[i],ver1,ver2))
    return d_out

#rawData = sc.textFile(data, 1)
#NumPoints = rawData.count()

kv = file.flatMap(f1).reduceByKey(lambda x,y:x+y).flatMap(f2)
#print kv.collect()
kv.saveAsTextFile(sys.argv[2])




# In[ ]:



