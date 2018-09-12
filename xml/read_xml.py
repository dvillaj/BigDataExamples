from pyspark.sql import HiveContext
from pyspark import SparkContext

sc = SparkContext(appName = "Read XML")
log4jLogger = sc._jvm.org.apache.log4j 
log = log4jLogger.LogManager.getLogger(__name__) 

sqlContext = HiveContext(sc)

log.warn("Lectura de la informacion")

df = sqlContext.read.format("com.databricks.spark.xml") \
    .options(rowTag="Book") \
    .load("file:///home/cloudera/Hadoop/xml/books.xml")

df.printSchema()
df.show()
