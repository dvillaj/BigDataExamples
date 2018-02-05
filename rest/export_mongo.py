import pymongo
import pymongo_spark
from pyspark import SparkContext
from pymongo import MongoClient

# Set up Mongo
client = MongoClient() 
db = client.data

# Important: activate pymongo_spark.
pymongo_spark.activate()

sc = SparkContext(appName = "Export to MongoDB")

log4jLogger = sc._jvm.org.apache.log4j 
log = log4jLogger.LogManager.getLogger(__name__) 

db.executives.drop()

csv_lines = sc.textFile("file:///home/cloudera/Hadoop/rest/data/example.csv")
data = csv_lines.map(lambda line: line.split(","))
schema_data = data.map(lambda x: {'name': x[0], 'company': x[1], 'title': x[2]})
schema_data.saveToMongoDB('mongodb://localhost:27017/data.executives')

log.warn("Done!")