from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext()
ssc = StreamingContext(sc, 5)
sts = ssc.socketTextStream('localhost', 9999)
fm = sts.flatMap(lambda x: x.split(' '))
fm.pprint()

ssc.start()
ssc.awaitTermination()