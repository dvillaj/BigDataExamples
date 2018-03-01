# encoding: utf-8

from pyspark.sql import HiveContext
from pyspark import SparkContext
from datetime import datetime
from pyspark.sql.types import *

def str_to_date(time_str):
    return datetime.strptime(time_str, '%Y-%m-%dT%H:%M:%SZ')

def split_text(text):
    return [w.lower() for w in text.split(' ')]
    
sc = SparkContext(appName = "Reviews App")
log4jLogger = sc._jvm.org.apache.log4j 
log = log4jLogger.LogManager.getLogger(__name__) 

sqlContext = HiveContext(sc)

log.warn("Lectura de los datos en formato JSON")

datos = sqlContext.read.json("/raw/reviews")
datos.registerTempTable("reviews_data")

log.warn("Proceso")
sqlContext.udf.register("str_to_date", str_to_date, TimestampType())
sqlContext.udf.register("split_text", split_text, ArrayType(StringType()))
df = sqlContext.sql("""
with datos as (
    SELECT 
        r.businessUnit.id as businessUnit_id,
        r.consumer.id as consumer_id,
        explode(split_text(r.text)) as word 
    FROM reviews_data
         LATERAL VIEW explode(reviews) adTable AS r
)
select businessUnit_id, consumer_id, word
from datos
where word <> ''
    """)

df.show()
df.printSchema()

print("Total Reviews: %d" % df.count())

log.warn("Salida a un fichero Parquet")

df.write.parquet("/data/words", mode="overwrite")

log.warn("Done")