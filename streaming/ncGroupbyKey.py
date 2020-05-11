from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext(master='local[4]')
ssc = StreamingContext(sc, 5)
sts = ssc.socketTextStream('localhost', 9999)

fm = sts.map(lambda x: (x, 1)).groupByKey().mapValues(list)
fm.pprint()

ssc.start()
ssc.awaitTermination()