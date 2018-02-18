# encoding: utf-8

from pyspark.sql import SQLContext
from pyspark import SparkContext

from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator


sc = SparkContext(appName = "Model App")
log4jLogger = sc._jvm.org.apache.log4j 
log = log4jLogger.LogManager.getLogger(__name__) 

sqlContext = SQLContext(sc)


log.warn("Lectura de la informacion")
df = sqlContext.read.json("/raw/msmk")
df.registerTempTable("jsons")
df.printSchema()

sqlContext.sql("select text, retweet_count from jsons").show()
log.warn("Total : %d" % df.count())

log.warn("Modelado")

tokenizer = Tokenizer().setInputCol("text").setOutputCol("palabras")
tf = HashingTF().setInputCol("palabras").setOutputCol("features")
regresion = LinearRegression(featuresCol="features",labelCol="retweet_count")
pipeline = Pipeline().setStages([tokenizer, tf, regresion])

params = ParamGridBuilder() \
    .addGrid(tf.numFeatures, range(10000,90000,10000)) \
    .addGrid(regresion.maxIter, range(30, 300, 50)) \
    .build()

evaluator = RegressionEvaluator(predictionCol="prediction", 
        labelCol="retweet_count", 
        metricName="mse")

validador = CrossValidator() \
    .setEstimator(pipeline) \
    .setEstimatorParamMaps(params) \
    .setEvaluator(evaluator)


train, test = df.randomSplit([0.7, 0.3])
pipeline_fitted = validador.fit(train)

resultado = evaluator.evaluate(pipeline_fitted.transform(test))
log.warn("Evaluación del modelo: %f" % resultado)

log.warn("Persistencia del modelo")
model = pipeline_fitted.bestModel
model.write().overwrite().save("/models/best_model")

log.warn("All Done!")
