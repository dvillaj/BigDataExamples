import sys, os, re

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import argparse

def proceso(time, rdd):
  print("========= %s =========" % str(time))
  lines = rdd.map(lambda x: x[1])
  lines.cache()

  total = lines.count()
  counts = lines.flatMap(lambda line: line.split(" ")) \
      .map(lambda word: (word, 1)) \
      .reduceByKey(lambda a, b: a+b) \
      .sortBy(lambda a: -a[1])

  print("Total de mensajes = %d " % total)
  if total > 0:
    print("Top 5 = %s " % str(counts.take(5)))



parser = argparse.ArgumentParser()
parser.add_argument('topic', help="Tópico Kafka")
args = parser.parse_args()

if args.topic is None:
    parser.error("Es necesario especificar un tópico kafka!")
    sys.exit(1)


# Process data every 10 seconds
PERIOD=10
ZOOKEEPER='localhost:2181'
BROKERS='localhost:9092'
TOPIC=args.topic
GROUP_ID='classic.group'

if __name__ == "__main__":
  conf = SparkConf().set("spark.default.parallelism", 1)

  sc = SparkContext(appName="PythonStreaming Kafka WordCount")
  ssc = StreamingContext(sc, PERIOD)

  kafkaParams= {"metadata.broker.list": BROKERS, "group.id": GROUP_ID, 
        "auto.offset.reset" : "smallest"}
  stream = KafkaUtils.createDirectStream(ssc, [TOPIC], kafkaParams)
  stream.foreachRDD(proceso)

  ssc.start()
  ssc.awaitTermination()