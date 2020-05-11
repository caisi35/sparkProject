from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext()
ssc = StreamingContext(sc, 5)
sts = ssc.socketTextStream('localhost', 9999)

set = sts.map(lambda x: (x, 1))
set.pprint()

ssc.start()
ssc.awaitTermination()