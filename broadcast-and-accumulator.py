from pyspark import SparkContext
sc = SparkContext("local", "broadcast app")

''' pyspark Broadcast class

class pyspark.Broadcast (
   sc = None,
   value = None,
   pickle_registry = None,
   path = None
)

Broadcast variable has an attribute called value
- value is used to store the data and return a broadcasted value
'''

words_new = sc.broadcast(["scala", "java", "hadoop", "spark", "akka"])
print("Broadcast words_new: %s" % (words_new))
data = words_new.value
# prints the values of the Braodcast object
print("Stored data -> %s" % (data))
elem = words_new.value[2]
print("Printing a particular element in RDD -> %s" % (elem))


''' Accumulator
    class pyspark.Accumulator(aid, value, accum_param)
- has an attribute called value similar to broadcast variable
- but value is only usable in a driver program
'''

num = sc.accumulator(10)
print("accumulator num -> %s" % (num))

def f(x):
    global num
    num += x

rdd = sc.parallelize([20, 30, 40, 50])
rdd.foreach(f)
final = num.value
print("Accumulated value is -> %i" % (final))
