from pyspark import SparkContext
import re, sys

sc = SparkContext("local", "Max Temperature")

rdd = sc.textFile(sys.argv[1]) \
     .map(lambda s: (s[15:19], s[87:92], s[92:93])) \
     .filter(lambda (year, temp, q): temp != "+9999" and re.match("[01459]", q)) \
     .map(lambda (year, temp, q): (int(year), int(temp))) \
     .reduceByKey(max) 

rdd.saveAsTextFile(sys.argv[2])
