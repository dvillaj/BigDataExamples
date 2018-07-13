from pyspark.sql import HiveContext
from pyspark import SparkContext
from pyspark.sql.types import *
import re

def python_camel_case_split(word):
    def split(identifier):
        matches = re.finditer('.+?(?:(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|$)', identifier)
        return [m.group(0) for m in matches]

    if (word[0] == '#'):
        return split(word[1:])
    else:
        return word.split(' ')


sc = SparkContext(appName = "Trends")
log4jLogger = sc._jvm.org.apache.log4j 
log = log4jLogger.LogManager.getLogger(__name__) 

sqlContext = HiveContext(sc)


log.warn("Lectura de la informacion")
df = sqlContext.read.json("/raw/trends/twitter-trends.json")
df.registerTempTable("raw_trends")
df.printSchema()

log.warn("Total")
results = sqlContext.sql("SELECT count(*) total from raw_trends")
results.show()


log.warn("Procesamiento")
sqlContext.udf.register("camel_split_function", 
    python_camel_case_split, ArrayType(StringType()))

df = sqlContext.sql(""" 
    with data as (
        SELECT created_at,
            explode(locations) as locations,
            trends
        FROM raw_trends
    ), data2 as (
        select created_at,locations.name as location, explode(trends) as trend
        from data
    ), data3 as (
    select substr(created_at, 1, 10) as dia,
        location, 
        trend.name,
        trend.tweet_volume as volumen,
        explode(camel_split_function(trend.name)) as word
    from data2
    ), data4 as (
        select dia, 
            volumen, 
            word,
            row_number() over (partition by dia, word order by volumen desc) as index
        from data3
    ) 
    select dia, volumen, word
    from data4
    where index = 1 and word <> ''
""")

df.registerTempTable("data")
df.printSchema()
df.show()


log.warn("Datos procesados")
sqlContext.sql("""
    select * from data
    limit 10
    """)

log.warn("Mongo")
df.write.format("com.mongodb.spark.sql.DefaultSource").mode("overwrite").save()

log.warn("Done")
