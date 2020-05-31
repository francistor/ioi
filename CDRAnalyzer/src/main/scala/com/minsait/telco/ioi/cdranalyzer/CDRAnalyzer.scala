package com.minsait.telco.ioi.cdranalyzer

// https://www.elastic.co/guide/en/elasticsearch/hadoop/master/spark.html#spark-streaming
// https://github.com/sksamuel/elastic4s


import org.apache.spark._
import org.apache.spark.sql.{ForeachWriter, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

import scala.sys.process._

object CDRAnalyzer extends App {

  case class AggMetric1(window: (java.sql.Timestamp, java.sql.Timestamp), aggField1: String, values: Array[Long], sampleSize: Int)
  case class AggMetricStats1(aggField1: String, mean: Double, stdDev: Double, samples: Int)
  case class KafkaMetric(key: String, value: String)

  evalAnomalies()
  System.exit(0)

  private val conf = new SparkConf()
    .setMaster("local[4]")
    .setAppName("CDRAnalyzer")
    .set("es.index.auto.create", "true")

  "rm -rf /tmp/spark/checkpoints" !

  // Use this version
  structured()


  /**
   * Version with structured streaming
   */
  def structured(): Unit = {
    val ss = SparkSession.builder().config(conf).getOrCreate()

    import ss.implicits._

    ///////////////////////////////////////////////////////////////
    // Raw CDR Stream --> 10s aggregate
    ///////////////////////////////////////////////////////////////

    val cdrStringDF = ss
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "cdr")
      .load()

    val cdrStreamDS = cdrStringDF.as[(String, String, String, Int, Long, java.sql.Timestamp, Int)].map(item => CDR.fromString(item._2))

    val agg_cdr_alive_bras_10s = cdrStreamDS
      .withWatermark("date", "3 seconds")
      .filter($"acctStatusType" === "Alive")
      .groupBy(window($"date", "10 seconds", "5 seconds"), $"nasIPAddress".as("aggField1"))
      .agg(count("nasIPAddress").as("value"))
      .withColumn("metricName", lit("agg_cdr_alive_bras_10s"))
      .withColumn("timestamp", $"window.end")
      .as[Metric1]

    val debug_output_agg_cdr_alive_bras_10s = agg_cdr_alive_bras_10s
      .writeStream
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .foreachBatch((ds, _) => {
        ds.show(10, truncate = false)
        println("number of agg", ds.count + " " + Thread.currentThread().getName)
      })
      .start()

    val es_output_agg_cdr_alive_bras_10s = agg_cdr_alive_bras_10s
      .withColumn("nasIPAddress", $"aggField1")
      .writeStream
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("checkpointLocation", "/tmp/spark/checkpoints/es_output_agg_cdr_alive_bras_10s")
      .format("es")
      .start("agg_cdr")

    val kafka_output_agg_cdr_alive_bras_10s = agg_cdr_alive_bras_10s.map(aggMetric => KafkaMetric(s"${aggMetric.timestamp}-${aggMetric.aggField1}", s"agg_cdr_alive_bras_10s,${aggMetric.timestamp},${aggMetric.aggField1},${aggMetric.value}"))
      .writeStream
      .outputMode("append")
      .option("checkpointLocation", "/tmp/spark/checkpoints/kafka_output_agg_cdr_alive_bras_10s")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .format("kafka")
      .option("topic", "agg_cdr")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .start()

    ///////////////////////////////////////////////////////////////
    // 10s agg CDR Stream --> 1m aggregate
    ///////////////////////////////////////////////////////////////
    val aggCDRStreamDF = ss
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "agg_cdr")
      .load()

    val aggCDRStreamDS = aggCDRStreamDF.as[(String, String, String, Int, Long, java.sql.Timestamp, Int)].map(item => Metrics.m1FromString(item._2))

    val agg2_cdr_alive_bras_1m_DS = aggCDRStreamDS
      .filter($"metricName" === "agg_cdr_alive_bras_10s")
      .withWatermark("timestamp", "30 seconds")
      .groupBy(window($"timestamp", "1 minute","30 seconds"), $"aggField1")
      .agg(collect_list("value").as("values"))
      .withColumn("sampleSize", lit(12))
      .as[AggMetric1].map(
        aggMetric => {
          // Fill missing values
          val size = aggMetric.values.length
          val filledValues = aggMetric.values ++ Array.fill[Long](aggMetric.sampleSize - size)(0)
          val mean = aggMetric.values.sum / aggMetric.sampleSize
          val stdDev = math.sqrt(filledValues.map(v => (v - mean) * (v - mean)).sum / aggMetric.sampleSize)
          AggMetricStats1(aggMetric.aggField1, mean, stdDev, size)
        })


    val debug_output_agg_cdr_alive_bras_1m_DS = agg2_cdr_alive_bras_1m_DS
      .writeStream
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .foreachBatch((ds, _) => {
        ds.show(200, truncate = false)
        println("number of agg2", ds.count)
      })
      .start()

    val es_output_agg_cdr_alive_bras_1m_DS = agg2_cdr_alive_bras_1m_DS
      .withColumn("nasIPAddress", $"aggField1")
      .withColumn("timestamp", lit(new java.sql.Timestamp(System.currentTimeMillis())))
      .writeStream
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .option("checkpointLocation", "/tmp/spark/checkpoints/es_output_agg_cdr_alive_bras_1m_DS")
      .format("es")
      .start("agg2_cdr")



    // Wait for termination
    kafka_output_agg_cdr_alive_bras_10s.awaitTermination()
  }

  // http://localhost:9200/agg2_cdr/_search?default_operator=AND&q= +nasIPAddress:212.230.100.102 +timestamp:>1590927410825
  def evalAnomalies(): Unit = {
    import com.sksamuel.elastic4s.ElasticDsl._
    import com.sksamuel.elastic4s.http.JavaClient
    import com.sksamuel.elastic4s.requests.searches.SearchResponse
    import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties, RequestFailure, RequestSuccess}

    val props = ElasticProperties("http://localhost:9200")
    val client = ElasticClient(JavaClient(props))

    val resp = client.execute {
      search("agg2_cdr").query("default_operator=AND&q= +nasIPAddress:212.230.100.102 +timestamp:>1590927410825")
    }.await

    // resp is a Response[+U] ADT consisting of either a RequestFailure containing the
    // Elasticsearch error details, or a RequestSuccess[U] that depends on the type of request.
    // In this case it is a RequestSuccess[SearchResponse]

    println("---- Search Results ----")
    resp match {
      case failure: RequestFailure => println("We failed " + failure.error)
      case results: RequestSuccess[SearchResponse] =>
        val a = results.result.hits.hits
        // get _source Map[String, AnyRef]
        println(results.result.hits.hits.toList)
        println(System.currentTimeMillis())
      case results: RequestSuccess[_] => println(results.result)
    }
  }
}



