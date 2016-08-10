#python

from pyspark import SparkContext

# access a hadoop hdfs uri
filename = "hdfs://r741.pvt.bridges.psc.edu:/datasets/plays/*"

# establish a spark context on our server.
sc = SparkContext("spark://r741.pvt.bridges.psc.edu:7077","SparkExample")

# could also do it locally on your laptop etc.:
#sc = SparkContext("local","SparkExample")

# cache the data in memory to avoid recomputation
filedata = sc.textFile(filename).cache()

numAs = filedata.filter(lambda s: 'a' in s).count()
numBs = filedata.filter(lambda s: 'b' in s).count()

print "Lines with a: %s, lines with b: %s" % (numAs, numBs)
