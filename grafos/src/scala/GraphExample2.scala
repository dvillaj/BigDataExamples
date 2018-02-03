package scala

import org.apache.spark._
import org.apache.spark.graphx._
import org.slf4j.LoggerFactory

import org.apache.spark.rdd.RDD
import scala.util.MurmurHash

// http://sparktutorials.net/analyzing-flight-data:-a-gentle-introduction-to-graphx-in-spark

object GraphExample2 {
  private val logger = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

      logger.warn("Iniciando el proceso...")
      val spark_conf = new SparkConf().setAppName("Graph Example")
      val sc = new SparkContext(spark_conf)

      val sqlContext= new org.apache.spark.sql.SQLContext(sc)
      import sqlContext.implicits._

      logger.warn("Preprocesado de la información")
      val df_raw =  sqlContext.read.format("com.databricks.spark.csv")
              .option("header", "false")
              .option("inferSchema", "true")
              .option("delimiter", " ")
              .load("/raw/graph/metadata-raw.txt")
              .toDF(Seq("Origin", "Dest") : _*)

      df_raw.printSchema()
      df_raw.show()

      val dataFromTo = df_raw.select($"Origin",$"Dest")
      val itemCodes: RDD[String] = df_raw.select($"Origin", $"Dest").flatMap(x => Iterable(x(0).toString, x(1).toString))

      val itemVertices: RDD[(VertexId, String)] = itemCodes.distinct().map(x => (MurmurHash.stringHash(x), x))
      val defaultItem = ("Missing")

      val itemEdges: RDD[Edge[PartitionID]] = dataFromTo.map(
         x => ((MurmurHash.stringHash(x(0).toString),
                MurmurHash.stringHash(x(1).toString)), 1))
         .reduceByKey(_+_)
         .map(x => Edge(x._1._1, x._1._2,x._2))

      val graph = Graph(itemVertices, itemEdges, defaultItem)
      graph.persist()

    logger.warn("Estadística básica")
    logger.warn(s"Cuantos Nodos = ${graph.numVertices}")
    logger.warn(s"Cuantas Relaciones = ${graph.numEdges}")

    logger.warn("Top 3")
    val top: Array[String] = graph.triplets
        .sortBy(_.attr, ascending=false)
        .map(triplet => s"There were ${triplet.attr.toString} calls from ${triplet.srcAttr} to ${triplet.dstAttr}")
      .take(3)

    top.foreach(logger.warn)

    logger.warn("Lowest 3")
    val lowest: Array[String] = graph.triplets
      .sortBy(_.attr)
      .map(triplet => s"There were ${triplet.attr.toString} calls from ${triplet.srcAttr} to ${triplet.dstAttr}")
      .take(3)

    lowest.foreach(logger.warn)

    val item: Array[(VertexId, (PartitionID, String))] = graph.inDegrees
                .join(itemVertices)
                .sortBy(_._2._1, ascending=false)
                .take(1)

    logger.warn(s"Elemento con mas llamadas = ${item.toString}")

    val ranks = graph.pageRank(0.0001).vertices
    val ranksAndAirports = ranks
        .join(itemVertices)
        .sortBy(_._2._1, ascending=false)
        .map(_._2._2)

    ranksAndAirports.take(5).foreach(logger.warn)

  }
}