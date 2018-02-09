import pymongo_spark
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, OffsetRange, TopicAndPartition
import sys, os, re
import json

# Process data every 10 seconds
PERIOD=1
BROKERS='localhost:9092'
TOPIC='flume'

if __name__ == "__main__":
    #conf = SparkConf().set("spark.default.parallelism", 1)
    sc = SparkContext(appName="PythonStreaming Export to Mongo")

    log4jLogger = sc._jvm.org.apache.log4j 
    log = log4jLogger.LogManager.getLogger(__name__) 

    ssc = StreamingContext(sc, PERIOD)

    # Lee la partición 0 desde el principio
    kafkaParams = {"metadata.broker.list": BROKERS, "group.id": "0"}
    start = 0
    partition = 0
    topicPartion = TopicAndPartition(TOPIC, partition)
    fromOffset = {topicPartion: long(start)}

    stream = KafkaUtils.createDirectStream(ssc, [TOPIC],kafkaParams, fromOffsets=fromOffset)
    object_stream = stream.map(lambda x: json.loads(x[1]))
    object_stream.saveToMongoDB('mongodb://localhost:27017/data.streaming')
    object_stream.pprint()

    ssc.start()
    ssc.awaitTermination()
