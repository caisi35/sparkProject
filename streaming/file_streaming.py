from pyspark import SparkContext
from pyspark import SQLContext
from pyspark.streaming import StreamingContext

sc = SparkContext()
ssc = StreamingContext(sc,5)
lines = ssc.textFileStream("file:///home/caisi/PycharmProjects/sparkProject/streaming")
lines.pprint()
ssc.start()
ssc.awaitTermination()