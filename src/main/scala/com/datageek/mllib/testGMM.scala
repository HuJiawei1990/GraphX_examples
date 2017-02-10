package com.datageek.mllib

/**
  * Created by Administrator on 2017/2/10.
  */
import org.apache.spark._
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.Vector
object testGMM {
  // spark initialization
  val conf  = new SparkConf().setAppName("GMM").setMaster("local")
  val sc = new SparkContext(conf)
  def main(arg: Array[String]): Unit = {

  }
}
