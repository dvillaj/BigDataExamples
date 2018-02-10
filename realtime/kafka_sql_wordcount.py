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
PERIOD=10
BROKERS='localhost:9092'
TOPICS = ['flume']
GROUP_ID='sql'
APP_NAME = 'TwitterStreamSql'
CHECKPOINT = '/tmp/%s' % APP_NAME
STREAM_CONTEXT_TIMEOUT=70

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
        select word, count(*) as total 
        from words 
        group by word
        order by count(*) desc
        """)

        wordCountsDataFrame.show()
    except:
        pass

def main(stream):
    words = stream.map(lambda x: x[1])\
                   .flatMap(lambda line: line.split(" "))
    words.foreachRDD(process)

def functionToCreateContext():
    sc = SparkContext(appName=APP_NAME)
    ssc = StreamingContext(sc, PERIOD)

    offsets = {TopicAndPartition(topic, 0): long(0) for topic in TOPICS}
    kafkaParams= {"metadata.broker.list": BROKERS, "group.id": GROUP_ID, 
        "auto.offset.reset" : "smallest"}
    
    stream = KafkaUtils.createDirectStream(ssc, TOPICS, kafkaParams, offsets)
    main(stream)

    ssc.checkpoint(CHECKPOINT)
    return ssc


if __name__ == "__main__":
    
    context = StreamingContext.getOrCreate(CHECKPOINT, functionToCreateContext)

    context.start()
    context.awaitTermination(timeout=STREAM_CONTEXT_TIMEOUT)
    context.stop()