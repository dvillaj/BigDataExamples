# encoding: utf-8

from pyspark.sql import HiveContext
from pyspark import SparkContext

sc = SparkContext(appName = "Simple App")
log4jLogger = sc._jvm.org.apache.log4j 
log = log4jLogger.LogManager.getLogger(__name__) 

sqlContext = HiveContext(sc)

log.warn("Comunidades autónomas")
results = sqlContext.sql("SELECT * from precios.ComunidadesAutonomas")
results.show()


log.warn("Lectura de los Precios en formato JSON")

precios = sqlContext.read.json("/raw/precios")
precios.registerTempTable("tabla_precios")
precios.printSchema()


log.warn("¿Donde está el precio más barato de Madrid?")

results = sqlContext.sql("""
    SELECT p.localidad, 
           p.r_tulo as rotulo,
           p.precio_gasoleo_a as diesel
    FROM tabla_precios p
    INNER JOIN precios.ComunidadesAutonomas c ON 
        (p.idccaa = c.IDCCAA)
    WHERE c.CCAA = "Madrid"
        and p.precio_gasoleo_a is not null
    ORDER BY p.precio_gasoleo_a asc 
    LIMIT 5
""")
results.show()
