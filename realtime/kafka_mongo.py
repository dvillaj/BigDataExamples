# encoding: utf-8

from pymongo import MongoClient
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, OffsetRange, TopicAndPartition
from datetime import datetime
import argparse
import sys, os, re
import json


def process(time, rdd):
    print("========= %s =========" % str(time))

    def process_json(json_string):
        doc = json.loads(json_string.decode('utf-8'))
        doc['_id'] = doc['id']
        doc['timestamp'] = datetime.strptime(doc['timestamp'], '%Y-%m-%d %H:%M:%S.%f')
        doc.pop('id', None)
        return doc

    def saveToMongo(doc):
        client = MongoClient('localhost',27017) 
        db = client.get_database('data')
        collection = db.get_collection('streaming')

        id = {'_id':doc['_id']}
        doc_db = collection.find_one(id)
        if doc_db is None or doc_db['timestamp'] < doc['timestamp']:
            collection.update(id, doc , upsert=True)

    try:
        rdd = rdd.map(lambda x: process_json(x[1]))
        rdd.foreach(saveToMongo)

        print("Saving %d messages to MongoDB" % rdd.count())

    except:
        pass



parser = argparse.ArgumentParser()
parser.add_argument('topic', help="Tópico Kafka")
args = parser.parse_args()

if args.topic is None:
    parser.error("Es necesario especificar un tópico kafka!")
    sys.exit(1)
    
# Process data every 10 seconds
PERIOD=1
BROKERS='localhost:9092'
TOPIC=args.topic
GROUP_ID='group.1'
APP_NAME = 'KafkaStreamMongo'

if __name__ == "__main__":

    #pymongo_spark.activate()

    #conf = SparkConf().set("spark.default.parallelism", 1)
    sc = SparkContext(appName=APP_NAME)

    log4jLogger = sc._jvm.org.apache.log4j 
    log = log4jLogger.LogManager.getLogger(__name__) 

    ssc = StreamingContext(sc, PERIOD)

    # Lee la partición 0 desde el principio
    kafkaParams = {"metadata.broker.list": BROKERS, "group.id": GROUP_ID}
    start = 0
    partition = 0
    topicPartion = TopicAndPartition(TOPIC, partition)
    fromOffset = {topicPartion: long(start)}

    stream = KafkaUtils.createDirectStream(ssc, [TOPIC], kafkaParams, fromOffsets=fromOffset)
    stream.foreachRDD(process)

    ssc.start()
    ssc.awaitTermination()