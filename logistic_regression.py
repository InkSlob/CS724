from collections import namedtuple
from math import exp
from os.path import realpath
import sys
import numpy as np
from pyspark import SparkContext

from pyspark.mllib.classification import LogisticRegressionWithSGD
from pyspark.mllib.regression import LabeledPoint
from numpy import array

def parsePoint(line):
    values = [float(x) for x in line.split(' ')]
    return LabeledPoint(values[0], values[1:])


if __name__ == "__main__":

    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: logistic_regression <file> <iterations>"
        exit(-1)

    sc = SparkContext(appName="PythonLR")
    
    # load and parse data
    data = sc.textFile(sys.argv[1])
    parsedData = data.map(parsePoint)

    #build model
    model = LogisticRegressionWithSGD.train(parsedData)
    print model

    # Evaluate the model on training data
    labelsAndPreds = parsedData.map(lambda p: (p.label, model.predict(p.features)))
    trainErr = labelsAndPreds.filter(lambda (v, p): v != p).count() / float(parsedData.count())
    print("Training Error = " + str(trainErr))
   
    sc.stop()
