# encoding: utf-8

from pyspark.sql import SQLContext
from pyspark import SparkContext
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('directory', help="Directorio donde se ubican los datos en HDFS")
args = parser.parse_args()

if args.directory is None:
    parser.error("Es necesario especificar la ruta donde se encuentran los datos en HDFS!")
    sys.exit(1)

sc = SparkContext(appName = "Show Struct App")
log4jLogger = sc._jvm.org.apache.log4j 
log = log4jLogger.LogManager.getLogger(__name__) 

sqlContext = SQLContext(sc)

try:
    datos = sqlContext.read.json(args.directory)
    datos.printSchema()
except:
    print("Error leyendo el fichero!")
