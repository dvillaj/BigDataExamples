from operator import add

textFile = sc.textFile("hdfs://localhost:8020/user/cloudera/shakespeare.txt")

counts = textFile.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)

counts.saveAsTextFile("hdfs://localhost:8020/user/cloudera/python_wordcount")

exit()


