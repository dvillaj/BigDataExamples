# encoding: utf-8

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

def proceso(stream):
    mappedStream = stream.map(lambda x: (x % 10, 1))
    reducedStream = mappedStream.reduceByKey(lambda a, b: a + b)
    reducedStream.pprint()

if __name__ == "__main__":

    sc = SparkContext(appName="PythonStreaming Queue Stream")
    ssc = StreamingContext(sc, 1)

    # Create the queue through which RDDs can be pushed to
    # a QueueInputDStream
    lista_rdds = []
    for i in range(5):
        rdd = ssc.sparkContext.parallelize([i for j in range(1, 1001)], 10)
        print("Rdd %d -  Elementos: %d, Muestra: %s" % (i, rdd.count(), rdd.take(5)))
        lista_rdds += [rdd]

    # Create the QueueInputDStream and use it do some processing
    stream = ssc.queueStream(lista_rdds)
    proceso(stream)
    

    ssc.start()
    ssc.awaitTermination()