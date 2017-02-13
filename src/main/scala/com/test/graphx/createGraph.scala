package com.test.graphx

/**
  * Created by hjw on 2017/2/7.
  */

//import GraphX tools
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.log4j.{Level, Logger}

object createGraph{
  // set the level of log to avoid to much information in console
  Logger.getLogger("org").setLevel(Level.WARN)

  // Initialization
  val conf = new SparkConf().setAppName("createGraph").setMaster("local")
  val sc = new SparkContext(conf)
  val debugMode = 1

  def main(args: Array[String]): Unit = {
    //val userGraph: Graph[(String, String), String]

    /* define vertices RDD structure
     * A vertex has (vID, (name, profession))
     */
    val users: RDD[(VertexId, (String, String))] = sc.parallelize(
      Array(
        (3L, ("cxin", "student")),
        (7L, ("ggsdf", "postdoc")),
        (5L, ("eee", "prof")),
        (2L, ("bbb", "prof"))
      )
    )
    // define default user if data is missing
    val defaultUser = ("NULL", "Missing")

    /* Create an RDD for edges
     * Each edge contains a source vertex ID & destination vertex ID
     * each edge has a structure (src vID, dst vID, relationship)
     */
    val relationships: RDD[Edge[(String, Int)]] = sc.parallelize(
      Array(Edge(3L, 7L, ("collab", 1)),
        Edge(5L, 3L, ("advisor", 1)),
        Edge(2L, 5L, ("colleague", 1)),
        Edge(5L, 7L, ("pi", 1)),
        Edge(4L, 3L, ("boss", 2))
      )
    )

    // create a initial graph model
    val graph = Graph(users, relationships, defaultUser)

    // ===========================================================
    // use the triplets view to create an overview of the whole graph
    val facts: RDD[String] = graph.triplets.map(
      triplet => triplet.srcAttr._1 + " is the " +
              triplet.attr._1 + " of " + triplet.dstAttr._1 +
              " with value " + triplet.attr._2
    )

    if (debugMode == 1) {
      println("********** hjw debug info **********")
      facts.collect.foreach(println(_))   //print result
    }

    // ==============================================================

  }
}
