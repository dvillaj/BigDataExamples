# -*- coding: utf-8 -*-

from pyspark.sql import HiveContext
from pyspark import SparkContext
from pyspark.sql.types import Row

sc = SparkContext(appName = "Export to MongoDB")

sqlContext = HiveContext(sc)

log4jLogger = sc._jvm.org.apache.log4j 
log = log4jLogger.LogManager.getLogger(__name__) 


log.warn("Lectura y análisis de datos")

csv_lines = sc.textFile("file:///home/cloudera/Hadoop/rest/data/example.csv")

def csv_to_row(line):
  parts = line.split(",")
  row = Row(
    name=parts[0],
    company=parts[1],
    title=parts[2]
  )
  return row

rows = csv_lines.map(csv_to_row)
rows_df = rows.toDF()
rows_df.registerTempTable("executives")

job_counts = sqlContext.sql("""
SELECT
  name,
  COUNT(*) AS total
FROM executives
GROUP BY name
""")
job_counts.show()

