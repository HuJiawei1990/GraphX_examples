package com.datageek

/**
  * Created by Administrator on 2017/2/10.
  */
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.log4j.{Logger, Level}
import org.apache.spark.rdd.RDD

import org.apache.spark.graphx.GraphLoader

object generateIdGraph {
  Logger.getLogger("org").setLevel(Level.WARN)
  val testMode = 1
  val debugMode = 0

  def main(args: Array[String]) : Unit = {
    // spark initialization
    val conf = new SparkConf().setAppName("generateTCUSTOMERId").setMaster("local")
    val sc = new SparkContext(conf)

    // ============================================
    // =========== Load graph from files ==========
    // ============================================

    // ====== Graph node : all ID information
    val allIdFile = "./src/test/data/allIdValues.csv"
    val allId : RDD[(VertexId, (String, String, String, String))] = sc.textFile(allIdFile).map{
      line => val fields = line.split(",")
        (fields(0).toLong,    // vertex ID
          (fields(1),         // source table
           fields(2),         // ID name (column name)
           fields(3),         // ID type
           fields(4))         // ID value
        )
    }

    if (testMode == 1){
      println("********** hjw debug info **********")
      println("*** There are " + allId.count() + " nodes.")
    }

    val defaultId = ("NULL", "NULL", "NULL", "NULL")

    // ===== Graph edges
    // ===== type 1: all ID from the same table
    val IdParisFile1 = "./src/test/data/associatedIdPairs.csv"
    val IdPairs1 : RDD[Edge[Int]] = sc.textFile(IdParisFile1).map {
      line => val fields = line.split(",")
        Edge( fields(1).toLong,           // source node ID
        fields(2).toLong,                 // destination node ID
        1                                 // relationship type => from the same table
      )
    }

    // ===== type 2: all ID have the same value
    val IdParisFile2 = "./src/test/data/associatedKeyByValue.csv"
    val IdPairs2 : RDD[Edge[Int]] = sc.textFile(IdParisFile2).map {
      line => val fields = line.split(",")
        Edge( fields(1).toLong,             // source node ID
          fields(2).toLong,                 // destination node ID
          2                                 // relationship type => from the same table
        )
    }
    if (testMode == 1) {
      println("********** hjw debug info **********")
      println("*** There are " + IdPairs1.count() + " connections of type 1.")
      println("*** There are " + IdPairs2.count() + " connections of type 2.")
    }

    val IdPairs = IdPairs1.union(IdPairs2)

    val graph = Graph(allId, IdPairs, defaultId)

    // ====== output the whole graph
    if (debugMode == 1) {
      println("********** hjw debug info **********")
      val details = graph.triplets.map(
        triplet => triplet.srcAttr._2 + " from table " + triplet.srcAttr._1 + " with values " + triplet.srcAttr._4 +
          " is connected with " +
          triplet.dstAttr._2 + " from table " + triplet.dstAttr._1 + " with values " + triplet.dstAttr._4 +
          " with type " + triplet.attr
      )
      details.collect().map(println(_))
    }

    val nonDirectedGraph = Graph(graph.vertices, graph.edges.union(graph.reverse.edges), defaultId)

    if (testMode == 1) {
      println("********** hjw debug info **********")
      println("*** There are " + nonDirectedGraph.edges.count() + " connections in final graph.")
    }
    // ====================================
    // ====== first exploration
    // ====================================

    // for a given ID value, find all its connected components
    val myIdValue = "D0BA55FB-3216-407D-95B0-B9C2C8BFF323"

    val selectedGraph = nonDirectedGraph.subgraph(
      vpred = (id, attr) => attr._2 == myIdValue
    )

    if (testMode == 1) {
      println("********** hjw debug info **********")
      println("*** all IDs connected with ID value = " + myIdValue)
      selectedGraph.vertices.map{
        vertex =>
          "Vertex ID " + vertex._1 + " from table " + vertex._2._1 +
            " in column " + vertex._2._2 + " with value " + vertex._2._4
      }.collect().map(println(_))
    }


  }
}
