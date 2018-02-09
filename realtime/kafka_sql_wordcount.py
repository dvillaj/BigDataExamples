from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
from pyspark.streaming.kafka import KafkaUtils, OffsetRange, TopicAndPartition


def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']

# Process data every 10 seconds
PERIOD=1
BROKERS='localhost:9092'
TOPIC='flume'

if __name__ == "__main__":
    sc = SparkContext(appName="Python Sql NetworkWordCount")
    ssc = StreamingContext(sc, PERIOD)

    stream = KafkaUtils.createDirectStream(ssc,[TOPIC], {"metadata.broker.list": BROKERS,
                                                         "group.id": "0",})

    words = stream.map(lambda x: x[1])\
                   .flatMap(lambda line: line.split(" "))

    def process(time, rdd):
        print("========= %s =========" % str(time))
        try:
            # Get the singleton instance of SQLContext
            sqlContext = getSqlContextInstance(rdd.context)

            # Convert RDD[String] to RDD[Row] to DataFrame
            rowRdd = rdd.map(lambda w: Row(word=w))
            wordsDataFrame = sqlContext.createDataFrame(rowRdd)

            # Register as table
            wordsDataFrame.registerTempTable("words")

            # Do word count on table using SQL and print it
            wordCountsDataFrame = sqlContext.sql("""
            select word, count(*) as total from words group by word
            """)

            wordCountsDataFrame.show()
        except:
            pass

    words.foreachRDD(process)
    ssc.start()
    ssc.awaitTermination()