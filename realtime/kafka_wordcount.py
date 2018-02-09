import sys, os, re

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, OffsetRange, TopicAndPartition

def count_word(stream):
  lines = stream.map(lambda x: x[1])
  counts = lines.flatMap(lambda line: line.split(" ")) \
      .map(lambda word: (word, 1)) \
      .reduceByKey(lambda a, b: a+b)
  counts.pprint()

# Process data every 10 seconds
PERIOD=10
ZOOKEEPER='localhost:2181'
BROKERS='localhost:9092'
TOPIC='flume'

if __name__ == "__main__":
  conf = SparkConf().set("spark.default.parallelism", 1)

  sc = SparkContext(appName="PythonStreaming Kafka WordCount")
  ssc = StreamingContext(sc, PERIOD)

  #stream = KafkaUtils.createStream(ssc, ZOOKEEPER, "spark-streaming-consumer", {TOPIC: 1})
  stream = KafkaUtils.createDirectStream(
    ssc,
    [TOPIC],
    {
      "metadata.broker.list": BROKERS,
      "group.id": "0",
    }
  )
  count_word(stream)

  ssc.start()
  ssc.awaitTermination()
