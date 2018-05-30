''' Serialization
- improves performance

MarshalSerializer:
- class pyspark.MarshalSerializer
- faster than PickleSerializer, but supports fewer datatypes

PickleSerializer:
- class pyspark.PickleSerializer
- supports nearly ay Python object
= might not be as fast as more specialized serializers
'''

from pyspark.context import SparkContext
from pyspark.serializers import MarshalSerializer
sc = SparkContext("local", "serialization app", serializer = MarshalSerializer())
print(sc.parallelize(list(range(1000))).map(lambda x: 2 * x).take(10))
sc.stop()
