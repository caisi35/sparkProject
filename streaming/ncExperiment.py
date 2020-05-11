from pyspark import SparkContext
from pyspark.streaming import StreamingContext


def update_func(new_value, last_sum):
    if last_sum is None:
        last_sum = 0
    return sum(new_value, last_sum)


sc = SparkContext()
ssc = StreamingContext(sc, 5)
ssc.checkpoint('/home/caisi/PycharmProjects/sparkProject/streaming/checkpointFolder')
ds = ssc.socketTextStream('localhost', 9999)
ds2 = ds.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1)).updateStateByKey(updateFunc=update_func)
ds2.pprint()
ssc.start()
ssc.awaitTermination()
