from pyspark import SparkContext
from operator import add

sc = SparkContext()
df = sc.textFile("file:///home/caisi/PycharmProjects/sparkProject/countWord/英文文档.txt")  # loading file
# print(df.count())

"""
flatMap() function split text file
map() function each word splice into tuples 
reduceByKey() function the same tuples add
sortBy() function sort the list to descending
"""
w_list = df.flatMap(lambda x:x.split(' '))\
    .map(lambda x:(x, 1))\
    .reduceByKey(add)\
    .sortBy(lambda x:x[1], ascending=False)\
    .collect()
# print(len(w_list))
# rdd = sc.parallelize(w_list)
# rdd.reduceByKey(add)
for i in w_list:
    print(i[0],":",i[1])
