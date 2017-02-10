package com.datageek

/**
  * Created by Administrator on 2017/2/10.
  */
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.log4j.{Logger, Level}

object generateIdGraph {
  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]) : Unit = {
    // spark initialization
    val conf = new SparkConf().setAppName("generateTCUSTOMERId").setMaster("local")
    val sc = new SparkContext(conf)

    // =============================
    val filePath = "./sec/test/date/T_CUSTOMER.csv"
    val T_CUSTOMER = sc.textFile(filePath).map{
      line => line.split(",")
    }
   }
}
