package scala

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory
import org.apache.hadoop.fs.{Path, FileSystem}
import java.net.URI

object GraphExample {
  private val logger = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

      logger.warn("Iniciando el proceso...")
      val spark_conf = new SparkConf().setAppName("Graph Example")
      val sc = new SparkContext(spark_conf)

      logger.warn("Preprocesado de la información")
      deleteDir(sc, "/tmp/graph")

      var vertices = sc.textFile("/raw/graph/metadata-raw.txt").
      flatMap { line => line.split("\\s+") }.distinct()

      vertices.map { vertex => vertex.replace("-", "") + "\t" + vertex }.
            saveAsTextFile("/tmp/graph/metadata-lookup")

      sc.textFile("/raw/graph/metadata-raw.txt").map { line =>
          var fields = line.split("\\s+")
          if (fields.length == 2) {
           fields(0).replace("-", "") + "\t" + fields(1).replace("-", "")
          }
      }.saveAsTextFile("/tmp/graph/metadata-processed")


      logger.warn("Proceso con grafos!")
      // Load the edges as a graph
      val graph = GraphLoader.edgeListFile(sc, "/tmp/graph/metadata-processed")
      // Run PageRank
      val ranks = graph.pageRank(0.0001).vertices
      // join the ids with the phone numbers
      val entities = sc.textFile("/tmp/graph/metadata-lookup").map { line =>
       val fields = line.split("\\s+")
       (fields(0).toLong, fields(1))
      }

      val ranksByVertex = entities.join(ranks).map {
        case (id, (vertex, rank)) => (rank, vertex)
      }

      logger.warn("Resultado:")
      // print out the top 5 entities
      println(ranksByVertex.sortByKey(false).take(5).mkString("\n"))
  }

  def deleteDir(sc : SparkContext, dirName : String) = {

      val conf = FileSystem.get(sc.hadoopConfiguration)
      val dir = new Path(dirName)

      if (conf.exists(dir)) {
        logger.info(s"Eliminando el directorio $dirName")
        conf.delete(dir,true) 
      }
  }
}