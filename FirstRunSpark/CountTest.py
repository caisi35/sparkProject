from pyspark import SparkContext


sc = SparkContext()
rdd1 = sc.parallelize([10,20,30,40,50])
count = rdd1.count()
print("rdd1 hava numbers:", count)