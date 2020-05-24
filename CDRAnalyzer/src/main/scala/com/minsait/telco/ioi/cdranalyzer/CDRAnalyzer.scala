package com.minsait.telco.ioi.cdranalyzer

import java.sql.Timestamp

import org.apache.spark._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


object CDRAnalyzer extends App{

  private val conf = new SparkConf().setMaster("local[2]").setAppName("CDRAnalyzer")

  structured()



  def structured(): Unit = {
    val ss = SparkSession.builder().config(conf).getOrCreate()

    import ss.implicits._

    def toCDRItems = ss.udf.register[
        (
          java.sql.Timestamp, // Date
          Long,   // Timestamp
          String, // AcctStatusType
          String, // AcctSessionId
          Long,   // SessionTime
          Long,   // OutputOctets
          Long,   // OutputGigawords
          Long,   // InputOctets,
          Long,   // InputGigawords
          String, // NASIdentifier
          String, // NASIPAddress
          Long,   // NASPort
          String, // ClientId
          String,   // AccessType
          String, // UserName
          String, // CallingStationId
          String, // CircuitId
          String, // RemoteId
          String, // FramedIPAddress,
          String, // ConnectInfo
          String, // TerminateCause
          String  // MAC

        ),
      String]("toCDRItems", (s: String) => {
        CDR.fromString(s).toTuple
    })

    val cdrStringDataFrame = ss
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "cdr")
      .load()

    val cdrDF = cdrStringDataFrame.withColumn("CDRItems", toCDRItems($"value"))
        .withColumn("Date", $"CDRItems._1")
        .withColumn("Timestamp", $"CDRItems._2")
        .withColumn("AcctStatusType", $"CDRItems._3")
        .withColumn("AcctSessionId", $"CDRItems._4")
        .withColumn("SessionTime", $"CDRItems._5")
        .withColumn("OutputOctets", $"CDRItems._6")
        .withColumn("OutputGigawords", $"CDRItems._7")
        .withColumn("InputOctets", $"CDRItems._8")
        .withColumn("InputGigawords", $"CDRItems._9")
        .withColumn("NASIdentifier", $"CDRItems._10")
        .withColumn("NASIPAddress", $"CDRItems._11")
        .withColumn("NASPort", $"CDRItems._12")
        .withColumn("ClientId", $"CDRItems._13")
        .withColumn("AccessType", $"CDRItems._14")
        .withColumn("UserName", $"CDRItems._15")
        .withColumn("CallingStationId", $"CDRItems._16")
        .withColumn("CircuitId", $"CDRItems._17")
        .withColumn("RemoteId", $"CDRItems._18")
        .withColumn("FramedIPAddress", $"CDRItems._19")
        .withColumn("ConnectInfo", $"CDRItems._20")
        .withColumn("TerminateCause", $"CDRItems._21")
        .withColumn("MAC", $"CDRItems._22")

    cdrDF.printSchema()

    val query = cdrDF.groupBy("NASIPAddress").count().writeStream
      .outputMode("complete")
      .format("console")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    query.awaitTermination()
  }

  def microBatch(): Unit = {
    val ssc = new StreamingContext(conf, Seconds(2))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "myGroupId",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("cdr")

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

}




