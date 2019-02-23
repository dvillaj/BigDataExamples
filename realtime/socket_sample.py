# encoding: utf-8

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

def proceso(stream):
    # Split each line into words
    words = stream.flatMap(lambda line: line.split(" "))

    # Count each word in each batch
    pairs = words.map(lambda word: (word, 1))
    wordCounts = pairs.reduceByKey(lambda x, y: x + y)

    # Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.pprint()



if __name__ == "__main__":

    # Create a local StreamingContext with two working thread and batch interval of 2 seconds
    sc = SparkContext(appName = "NetworkWordCount")
    ssc = StreamingContext(sc, 2)

    # Create a DStream that will connect to hostname:port, like localhost:9999
    stream = ssc.socketTextStream("localhost", 9999)
    proceso(stream)

    ssc.start()
    ssc.awaitTermination()