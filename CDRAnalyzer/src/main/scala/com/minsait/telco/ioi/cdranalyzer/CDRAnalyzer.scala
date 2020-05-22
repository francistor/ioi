package com.minsait.telco.ioi.cdranalyzer

import org.apache.spark._
import org.apache.spark.streaming._

object CDRAnalyzer extends App{

  private val conf = new SparkConf().setMaster("local[2]").setAppName("CDRAnalyzer")
  private val ssc = new StreamingContext(conf, Seconds(2))

  private val lines = ssc.socketTextStream("localhost", 9999)
  private val words = lines.flatMap(_.split(" "))

  words.map(w => (w, 1)).reduceByKey(_ + _).print()

  ssc.start()
  ssc.awaitTermination()
}
