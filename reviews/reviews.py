# encoding: utf-8

from pyspark.sql import HiveContext
from pyspark import SparkContext
from datetime import datetime
from pyspark.sql.types import *

def str_to_date(time_str):
    return datetime.strptime(time_str, '%Y-%m-%dT%H:%M:%SZ')


sc = SparkContext(appName = "Reviews App")
log4jLogger = sc._jvm.org.apache.log4j 
log = log4jLogger.LogManager.getLogger(__name__) 

sqlContext = HiveContext(sc)

log.warn("Lectura de los datos en formato JSON")

datos = sqlContext.read.json("/raw/reviews")
datos.registerTempTable("reviews_data")


log.warn("Proceso")


sqlContext.udf.register("str_to_date", str_to_date, TimestampType())
df = sqlContext.sql("""
SELECT 
    r.businessUnit.id as businessUnit_id,
    r.consumer.id as consumer_id,
    r.consumer.displayName as displayName,
    r.consumer.numberOfReviews as numberOfReviews,
    r.stars,
    r.title,
    r.text,
    r.language,
    str_to_date(r.createdAt) as createdAt,
    r.referralEmail,
    r.referenceId,
    r.isVerified
FROM reviews_data
     LATERAL VIEW explode(reviews) adTable AS r
    """)

df.show()
df.printSchema()

print("Total Reviews: %d" % df.count())

log.warn("Salida a un fichero Parquet")

df.write.parquet("/data/reviews", mode="overwrite")

log.warn("Done")
