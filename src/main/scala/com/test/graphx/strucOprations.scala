package com.test.graphx

/**
  * Created by hjw on 2017/2/9.
  */

//import GraphX tools
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import org.apache.log4j.{Level, Logger}

object strucOprations {
  // set the level of log to avoid to much information in console
  Logger.getLogger("org").setLevel(Level.WARN)

  // Initialization
  val conf = new SparkConf().setAppName("createGraph").setMaster("local")
  val sc = new SparkContext(conf)
  val debugMode = 1

  def main(args: Array[String]): Unit = {
    // ==========================================================
    // Generate a new graph
    // ==========================================================

    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof")),
        (4L, ("peter", "student"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi"),
        Edge(4L, 0L, "student"),   Edge(5L, 0L, "colleague")))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("NULL", "Missing")
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)

    // ==========================================================
    // print out the structure of graph using triplet
    println("********** hjw debug info **********")

    // Notice that there is a user 0 (for which we have no information) connected to users
    // 4 (peter) and 5 (franklin).
    graph.triplets.map(
      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
    ).collect.foreach(println(_))

    // ==========================================================
    // reverse
    // ==========================================================
    val reverseGraph = graph.reverse

    // ==========================================================
    // sub-graph
    // ==========================================================

    // Remove missing vertices as well as the edges to connected to them
    val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")

    // The valid sub-graph will disconnect users 4 and 5 by removing user 0
    // validGraph.vertices.collect.foreach(println(_))

    println("********** hjw debug info **********")

    // print out the new valid graph
    printGraph(validGraph)

  }

  def printGraph(inputGraph: Graph[(String, String), String]): Unit = {
    inputGraph.triplets.map(
      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
    ).collect.foreach(println(_))
  }

}


