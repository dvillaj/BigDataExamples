# encoding: utf-8

from pyspark.ml.pipeline import PipelineModel 
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import SQLContext
from pyspark import SparkContext

sc = SparkContext(appName = "Eval Model App")
log4jLogger = sc._jvm.org.apache.log4j 
log = log4jLogger.LogManager.getLogger(__name__) 

sqlContext = SQLContext(sc)

log.warn("Lectura de la informacion")
df = sqlContext.read.json("/raw/msmk")
df.registerTempTable("jsons")
dataset = sqlContext.sql("select text as texto, retweet_count as retweets from jsons")

sqlContext.sql("select sum(retweet_count) as sum_retweet_count from jsons").show()

model = PipelineModel.load("/models/best_model")

evaluator = RegressionEvaluator(predictionCol="prediction", 
        labelCol="retweets", 
        metricName="mse")

predicciones = model.transform(dataset)
resultado = evaluator.evaluate(predicciones)

log.warn("Evaluación del modelo: %f" % resultado)
predicciones.show()