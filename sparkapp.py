from pyspark import SparkContext
from operator import add

logfile = "README.md"
sc = SparkContext("local", "CitibikeApp")

logData = sc.textFile(logfile).cache()
# cache() method is a shorthand for using the default storage level
numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()
print( "Lines with a: %i, lines with b: %i" % (numAs, numBs))
create RDD words

# make an RDD called words
words = sc.parallelize (
   ["scala",
   "java",
   "hadoop",
   "spark",
   "akka",
   "spark vs hadoop",
   "pyspark",
   "pyspark and spark"]
)

counts = words.count()
print( "Number of elements in RDD -> %i" % (counts))

coll = words.collect()
print( "Elements in RDD -> %s" % (coll))

def f(x): print(x)
fore = words.foreach(f)

# filter: returns a new RDD
words_filter = words.filter(lambda x: 'spark' in x)
filtered = words_filter.collect()
print( "Fitered RDD -> %s" % (filtered))

# map: returns a new RDD
words_map = words.map(lambda x: (x, 1))
mapping = words_map.collect()
print( "Key value pair -> %s" % (mapping))

# reduce: returns an element in the RDD after applying the specified operations 
# make a nums RDD
nums = sc.parallelize([1, 2, 3, 4, 5])
adding = nums.reduce(add)
print( "Adding all the elements -> %i" % (adding))


