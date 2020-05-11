from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext(master='local[4]')
ssc = StreamingContext(sc, 5)
sts = ssc.socketTextStream('localhost', 9999)

fm = sts.flatMap(lambda x: x.split(' ')).map(lambda y:(y, 1)).reduceByKey(lambda x, y: x + y)
fm.pprint()

ssc.start()
ssc.awaitTermination()


# Hello pyspark streaming

