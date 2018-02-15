# encoding: utf-8

from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
from pyspark.streaming.kafka import KafkaUtils, OffsetRange, TopicAndPartition
from pyspark.ml.pipeline import PipelineModel 
from pymongo import MongoClient
from datetime import datetime
import argparse
import json

def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']

def getModel(sparkContext):
    if ('modelInstance' not in globals()):
        globals()['modelInstance'] = PipelineModel.load("/models/best_model")
    return globals()['modelInstance']

def process(time, rdd):

    def get_json(items):
        return { '_id': items[0], 
                 'texto':  items[1], 
                 'retweet_count': items[2], 
                 'usuario' : items[3],
                 'timestamp' : datetime.utcnow(),
                 'prediction' : items[4]
                 }

    def saveToMongo(doc):
        client = MongoClient('localhost',27017) 
        db = client.get_database('data')
        collection = db.get_collection('twitter')

        id = {'_id':doc['_id']}
        doc_db = collection.find_one(id)
        if doc_db is None:
            collection.update(id, doc , upsert=True)

    def process_json(json_string):
        doc = json.loads(json_string)

        new_doc = {}
        new_doc['id'] = doc['id']
        new_doc['texto'] = doc['text']
        new_doc['retweet_count'] = doc['retweet_count']
        new_doc['usuario'] = doc['user']['screen_name']

        return new_doc

    print("========= %s =========" % str(time))
    sqlContext = getSqlContextInstance(rdd.context)
    model = getModel(rdd.context)

    json_rdd = rdd.map(lambda x: process_json(x[1]))
    if not json_rdd.isEmpty():

        json_df = sqlContext.createDataFrame(json_rdd)
        predicciones_df = model.transform(json_df)

        predicciones_df.show()
        print("Count %d " % predicciones_df.count())

        predicciones_df.registerTempTable("predicciones")

        result_df = sqlContext.sql("""
            select id, texto, retweet_count, usuario, prediction
            from predicciones
        """)

        result_df.rdd.map(get_json).foreach(saveToMongo)

def functionToCreateContext():
    sc = SparkContext(appName=APP_NAME)
    ssc = StreamingContext(sc, PERIOD)

    offsets = {TopicAndPartition(topic, 0): long(0) for topic in TOPICS}
    kafkaParams= {"metadata.broker.list": BROKERS, "group.id": GROUP_ID, 
        "auto.offset.reset" : "smallest"}
    
    stream = KafkaUtils.createDirectStream(ssc, TOPICS, kafkaParams, offsets)
    stream.foreachRDD(process)

    ssc.checkpoint(CHECKPOINT)
    return ssc


parser = argparse.ArgumentParser()
parser.add_argument('topic', help="Tópico Kafka")
args = parser.parse_args()

if args.topic is None:
    parser.error("Es necesario especificar un tópico kafka!")
    sys.exit(1)


# Process data every 10 seconds
PERIOD=10
BROKERS='localhost:9092'
TOPICS = [args.topic]
GROUP_ID='group.1'
APP_NAME = 'TwitterStreamML'
CHECKPOINT = '/tmp/%s' % APP_NAME
STREAM_CONTEXT_TIMEOUT=70


if __name__ == "__main__":
    
    context = StreamingContext.getOrCreate(CHECKPOINT, functionToCreateContext)

    context.start()
    context.awaitTermination(timeout=STREAM_CONTEXT_TIMEOUT)
    context.stop()