# encoding: utf-8

from pyspark.ml.pipeline import PipelineModel 
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import HiveContext
from pyspark import SparkContext

sc = SparkContext(appName = "Eval Model App")
log4jLogger = sc._jvm.org.apache.log4j 
log = log4jLogger.LogManager.getLogger(__name__) 

sqlContext = HiveContext(sc)

log.warn("Lectura de la informacion")
df = sqlContext.read.json("/raw/msmk")
df.registerTempTable("jsons")
sqlContext.sql("""
    select retweet_count, count(*) as total
    from jsons
    group by retweet_count
    order by 2 desc
""").show()

log.warn("Aplicación del modelo")
model = PipelineModel.load("/models/best_model")

dataset = sqlContext.sql("""
    select  id as _id,
        text, 
        retweet_count,
        user.screen_name as user
    from jsons
    """)

predicciones = model.transform(dataset)
predicciones.show()

log.warn("Evaluación del modelo")
evaluator = RegressionEvaluator(predictionCol="prediction", 
        labelCol="retweet_count", 
        metricName="mse")

resultado = evaluator.evaluate(predicciones)
log.warn("Evaluación del modelo: %f" % resultado)

log.warn("Exportación de la información a MongoDB")
predicciones = predicciones.drop("features")
predicciones.printSchema()

predicciones.write \
    .format("com.mongodb.spark.sql.DefaultSource") \
    .option("uri", "mongodb://127.0.0.1") \
    .option("database", "model") \
    .option("collection", "twitter") \
    .option("replaceDocument", "false") \
    .mode("append") \
    .save()

log.warn("Done")
