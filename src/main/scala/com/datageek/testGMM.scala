package com.datageek

/**
  * Created by Administrator on 2017/2/10.
  */
import org.apache.spark._
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.log4j.{Level, Logger}

object testGMM {
  Logger.getLogger("org").setLevel(Level.WARN)
  // spark initialization
  val conf  = new SparkConf().setAppName("GMM").setMaster("local")
  val sc = new SparkContext(conf)


  def main(arg: Array[String]): Unit = {
    // Load and parse the data
    val fileName = "./src/test/data/mllib/gmm_data.txt"
    val data = sc.textFile(fileName)
    val parsedData = data.map(s => Vectors.dense(s.trim.split(' ').map(_.toDouble))).cache()

    // Cluster the data into three classes using GaussianMixture
    val gmm = new GaussianMixture().setK(3).run(parsedData)

    // output parameters of max-likelihood model
    println("********** hjw debug info **********")
    for (i <- 0 until gmm.k) {
      println("weight=%f\nmu=%s\nsigma=\n%s\n" format
        (gmm.weights(i), gmm.gaussians(i).mu, gmm.gaussians(i).sigma))
    }

    val preds = gmm.predict(parsedData)
    // preds.collect.map(println(_))

    val numClass = gmm.k

  }
}
