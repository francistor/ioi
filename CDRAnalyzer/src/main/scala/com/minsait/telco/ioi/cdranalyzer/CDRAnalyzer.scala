package com.minsait.telco.ioi.cdranalyzer

import java.sql.Timestamp

import org.apache.spark._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


object CDRAnalyzer extends App{

  private val conf = new SparkConf().setMaster("local[2]").setAppName("CDRAnalyzer")

  /*
  private val ss = SparkSession.builder().config(conf).getOrCreate()

  import ss.implicits._

  val cdrDataFrame = ss
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "cdr")
    .load()



  val query = cdrDataFrame.groupBy("value").count().writeStream
    .outputMode("complete")
    .format("console")
    .start()

  query.awaitTermination()

  /*
  cdrDataFrame.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String, String)]
*/
  */

  private val ssc = new StreamingContext(conf, Seconds(2))

  private val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "myGroupId",
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  private val topics = Array("cdr")

  val stream = KafkaUtils.createDirectStream[String, String](
    ssc,
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams)
  )

  stream.foreachRDD { rdd =>

    // Get the singleton instance of SparkSession
    val session = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
    import session.implicits._

    // Convert RDD[String] to DataFrame
    val cdrRDD = rdd.map(record => CDRSchema.stringToCDR(record.value))
    val cdrDF = session.createDataFrame(cdrRDD, CDRSchema.schema)
    println(cdrDF.count)

    /*
    // Create a temporary view
    wordsDataFrame.createOrReplaceTempView("words")

    // Do word count on DataFrame using SQL and print it
    val wordCountsDataFrame =
      spark.sql("select word, count(*) as total from words group by word")
    wordCountsDataFrame.show()

     */
  }

  // val a = stream.map(record => (record.key, record.value))

  ssc.start()
  ssc.awaitTermination()



}


