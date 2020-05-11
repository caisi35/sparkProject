from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext()
ssc = StreamingContext(sc, 5)
lines = ssc.socketTextStream('127.0.0.1', 9999)
# pprint data
# lines.pprint()
# groupByKey
# ds = lines.map(lambda x:(x,1)).groupByKey().mapValues()
# ds.pprint()
# reduceByKey
# rds = lines.map(lambda x:(x,3)).reduceByKey(lambda x,y:x*y)
# rds.pprint()
# reduceBeKey wordCount
rwc = lines.flatMap(lambda x:x.split(' ')).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)
rwc.pprint()
ssc.start()
ssc.awaitTermination()

