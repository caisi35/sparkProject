from pyspark import SparkContext
from operator import add

sc = SparkContext()
df = sc.textFile("file:///home/caisi/PycharmProjects/sparkProject/countWord/英文文档.txt")  # loading file
# print(df.count())
split_list = df.flatMap(lambda x:x.split(' ')).collect()    # split text file
# print(split_list)
rdd1 = sc.parallelize(split_list)
map_list = rdd1.map(lambda x:(x, 1)).collect()  # each word splice into tuples
# print(map_list)
rdd2 = sc.parallelize(map_list)
reduce_list = rdd2.reduceByKey(add).collect()   # the same tuples add
# print(reduce_list)
rdd3 = sc.parallelize(reduce_list)
sort_list = rdd3.sortBy(lambda x:x[1], ascending=False).collect()   # sort the list to descending
# print(len(sort_list))
print("Word Count Result:")
for i in sort_list:
    print(i[0],":",i[1])
