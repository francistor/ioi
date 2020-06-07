package com.minsait.telco.ioi.cdranalyzer

// https://www.elastic.co/guide/en/elasticsearch/hadoop/master/spark.html#spark-streaming
// https://github.com/sksamuel/elastic4s


import java.util.Properties

import com.sksamuel.elastic4s.ElasticDsl.indexInto
import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

import scala.concurrent.ExecutionContext.Implicits.global
import scala.sys.process._
import scala.util.{Failure, Success}
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.{Interaction, OneHotEncoderEstimator, StringIndexer, StringIndexerModel}
import org.apache.spark.sql.expressions.Window

object CDRAnalyzer extends App {


  val appConf = ConfigFactory.load()


  private val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("CDRAnalyzer")


  "rm -rf /tmp/spark/checkpoints" !

  val ss = SparkSession.builder().config(sparkConf).getOrCreate()


  badSessionPattern()


  def brasAlive(): Unit = {

    // Initialize Elastic
    {
      import com.sksamuel.elastic4s.ElasticDsl._
      val props = ElasticProperties(appConf.getString("elastic.url"))
      val client = ElasticClient(JavaClient(props))
      client.execute {
        createIndex("agg_cdr")
        createIndex("agg2_cdr")
      }.await
    }

    import ss.implicits._

    val brasAgg1Seconds = appConf.getInt("analyzer.brasAgg1Seconds")
    val brasAgg2Seconds = appConf.getInt("analyzer.brasAgg2Seconds")
    val brasStdDevAnomaly = appConf.getInt("analyzer.brasStdDevAnomaly")

    if(! (brasAgg2Seconds % brasAgg1Seconds == 0)) throw new Exception("brasAgg2Seconds is not a multiple of brasAgg1Seconds")

    ///////////////////////////////////////////////////////////////
    // Raw CDR Stream --> 1st order aggregate
    ///////////////////////////////////////////////////////////////

    val cdrStringDF = ss
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", appConf.getString("kafka.bootstrapServers"))
      .option("subscribe", "cdr")
      .load()

    val cdrStreamDS = cdrStringDF.as[(String, String, String, Int, Long, java.sql.Timestamp, Int)].map(item => CDR.fromString(item._2))

    // Aggregate count by NASIpAddress
    val agg1_cdr_alive_bras = cdrStreamDS
      .withWatermark("date", "3 seconds")
      .filter($"acctStatusType" === "Alive")
      .groupBy(window($"date", s"${2 * brasAgg1Seconds} seconds", s"$brasAgg1Seconds seconds"), $"nasIpAddress".as("aggField1"))
      .agg(count("nasIpAddress").as("value"))
      .withColumn("aggName", lit("agg1_cdr_alive_bras"))
      .withColumn("timestamp", $"window.end")
      .as[FOAggregation1]

    // Write to multiple outputs
    val agg_cdr_writer = agg1_cdr_alive_bras
      .writeStream
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(s"$brasAgg1Seconds seconds"))
      .foreachBatch((ds, _) => {
        println("------------- agg --------------")
        ds.show(10, truncate = false)

        ds.foreachPartition(aggList => {

          // ES
          val props = ElasticProperties(appConf.getString("elastic.url"))
          val client = ElasticClient(JavaClient(props))

          // Kafka
          val kafkaProperties = new Properties()
          kafkaProperties.put("bootstrap.servers", appConf.getString("kafka.bootstrapServers"))
          kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
          kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
          val producer = new KafkaProducer[String, String](kafkaProperties)

          val esOperationList = aggList.map(agg => {
            // Write to Kafka
            val record = new ProducerRecord[String, String]("agg_cdr", s"${agg.timestamp}-${agg.aggField1}", s"agg1_cdr_alive_bras,${agg.timestamp},${agg.aggField1},${agg.value}")
            producer.send(record)

            evalAnomaly(client, List("nasIpAddress"), List(agg.aggField1), "agg2_cdr_alive_bras", agg.value, brasStdDevAnomaly)

            agg match {
              case FOAggregation1(aggName, timestamp, value, aggField1) =>
                indexInto("agg_cdr").fields(Map(
                  "value" -> agg.value,
                  "timestamp" -> agg.timestamp.getTime,
                  "aggName" -> agg.aggName,
                  "aggField1" -> aggField1,
                  "nasIpAddress" -> aggField1
                ))
            }
          }).toList

          producer.close()

          import com.sksamuel.elastic4s.ElasticDsl._
          client.execute {
            bulk(esOperationList)
          }.onComplete {
              case Success(s) =>
                client.close()
              case Failure(e) =>
                println(e)
                client.close()
          }
        })
      }).start()


    ///////////////////////////////////////////////////////////////
    // agg1 CDR Stream --> agg2
    ///////////////////////////////////////////////////////////////

    // Read from Kafka
    val aggCDRStreamDF = ss
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", appConf.getString("kafka.bootstrapServers"))
      .option("subscribe", "agg_cdr")
      .load()

    val aggCDRStreamDS = aggCDRStreamDF.as[(String, String, String, Int, Long, java.sql.Timestamp, Int)].map(item => FOAggs.foAgg1FromString(item._2))

    val agg2_cdr_alive_bras_DS = aggCDRStreamDS
      .filter($"aggName" === "agg1_cdr_alive_bras")
      .withWatermark("timestamp", "5 seconds")
      .groupBy(window($"timestamp", s"${2 * brasAgg2Seconds} seconds",s"$brasAgg2Seconds seconds"), $"aggField1")
      .agg(collect_list("value").as("values"))
      .withColumn("sampleSize", lit(2 * brasAgg2Seconds / brasAgg1Seconds))
      .as[SOAggregation1Raw].map(
        agg2 => {
          // Fill missing values. Should be "sampleSize" values.
          val size = agg2.values.length
          val filledValues = agg2.values ++ Array.fill[Long](agg2.sampleSize - size)(0)
          val mean = agg2.values.sum / agg2.sampleSize
          val stdDev = math.sqrt(filledValues.map(v => (v - mean) * (v - mean)).sum / agg2.sampleSize)
          SOAggregation1Stats("agg2_cdr_alive_bras", agg2.window._2, agg2.aggField1, mean, stdDev, size)
        })

    agg2_cdr_alive_bras_DS
      .writeStream
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(s"$brasAgg2Seconds seconds"))
      .foreachBatch((ds, _) => {
        println("------------- agg2 --------------")
        ds.show(200, truncate = false)
        ds.foreachPartition(aggList => {

          // ES
          val props = ElasticProperties(appConf.getString("elastic.url"))
          val client = ElasticClient(JavaClient(props))

          val esOperationList = aggList.map {
            case agg@SOAggregation1Stats(aggName, timestamp, aggField1, mean, stdDev, samples) =>
              indexInto("agg2_cdr").fields(Map(
                "mean" -> mean,
                "stdDev" -> stdDev,
                "timestamp" -> timestamp.getTime,
                "aggName" -> aggName,
                "aggField1" -> aggField1,
                "nasIpAddress" -> aggField1
              ))
          }.toList

          import com.sksamuel.elastic4s.ElasticDsl._
          client.execute {
            bulk(esOperationList)
          }.onComplete {
            case Success(s) =>
              client.close()
            case Failure(e) =>
              println(e)
              client.close()
          }
        })
      })
      .start()


    // Wait for termination
    agg_cdr_writer.awaitTermination()
  }

  // http://localhost:9200/agg2_cdr/_search?default_operator=AND&q= +nasIpAddress:212.230.100.102 +timestamp:>1590927410825
  def evalAnomaly(client: ElasticClient, aggNames: List[String], aggValues: List[String], aggregationName: String, currentValue: Long, nStdDev: Int): Unit = {
    import com.sksamuel.elastic4s.ElasticDsl._
    import com.sksamuel.elastic4s.requests.searches.SearchResponse
    import com.sksamuel.elastic4s.{RequestFailure, RequestSuccess}

    val targetTimestamp = System.currentTimeMillis() - 1000 * 30 * 5

    val query = s"default_operator=AND&q= +timestamp:>$targetTimestamp" + aggNames.zip(aggValues).map{case (n, v) => s" +$n:$v"}.mkString("")

    client.execute {
      search("agg2_cdr").query(query)
    }.onComplete{
      case Success(resp) =>
        resp match {
          case failure: RequestFailure => println("Error searching Elastisearch" + failure.error)
          case results: RequestSuccess[SearchResponse] =>

            val hits = results.result.hits.hits.toList
            if(hits.nonEmpty) {
              val mostRecentHit = hits.reduceLeft((x, y) => if (x.sourceAsMap("timestamp").asInstanceOf[Long] > y.sourceAsMap("timestamp").asInstanceOf[Long]) x else y)
              val mean = mostRecentHit.sourceAsMap("mean").asInstanceOf[Double]
              val stdDev = mostRecentHit.sourceAsMap("stdDev").asInstanceOf[Double]
              val offset = System.currentTimeMillis() - mostRecentHit.sourceAsMap("timestamp").asInstanceOf[Long]
              if((mean - currentValue) > nStdDev * stdDev) println(s"Anomaly $aggregationName for ${aggNames.zip(aggValues).mkString(":")} value: $currentValue [$mean, $stdDev] offset: ${offset / 1000} seconds")
            }
        }
      case Failure(e) =>
        println("Error querying ES", e)
    }

  }

  def badSessionPattern(): Unit = {

    import ss.implicits._

    ///////////////////////////////////////////////////////////////
    // Raw CDR Stream --> 1st order aggregate
    ///////////////////////////////////////////////////////////////

    val termCauseAgg1Seconds = appConf.getInt("analyzer.termCauseAgg1Seconds")

    val cdrStringDF = ss
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", appConf.getString("kafka.bootstrapServers"))
      .option("subscribe", "cdr")
      .load()

    val cdrStreamDS = cdrStringDF.as[(String, String, String, Int, Long, java.sql.Timestamp, Int)].map(item => CDR.fromString(item._2))

    val agg1_cdr_stop_bras_termCause = cdrStreamDS
      .withWatermark("date", "10 seconds")
      .filter($"acctStatusType" === "Stop")
      .groupBy(window($"date", s"${2*termCauseAgg1Seconds} seconds", s"$termCauseAgg1Seconds seconds"),
        $"nasIpAddress".as("aggField1"),
        $"terminateCause".as("aggField2"))
      .agg(count($"*").as("value"))
      .withColumn("aggName", lit("agg1_cdr_stop_bras_termCause"))
      .withColumn("timestamp", $"window.end")
      .as[FOAggregation2]

    val strQuery = agg1_cdr_stop_bras_termCause
      .writeStream
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(s"$termCauseAgg1Seconds seconds"))
      .foreachBatch((ds, _) => {
        val df = ds.toDF
          .withColumn("sumPerBras", sum($"value").over(Window.partitionBy($"aggField1")))
          .withColumn("ratioInBras", $"value"/$"sumPerBras")

        // To assign float labels to the termination causes
        val termCauseIndexer = new StringIndexer().setInputCol("aggField2").setOutputCol("terminateCauseIndex")
        // To encode the float labels in a vector
        val termCauseEncoder = new OneHotEncoderEstimator().setDropLast(false).setInputCols(Array("terminateCauseIndex")).setOutputCols(Array("featuresNotScaled"))
        // Scale with the number of clients per BRAS. The scaled feature is "featuresUA". Not needed here
        val multiplier = new Interaction().setInputCols(Array("featuresNotScaled", "ratioInBras")).setOutputCol("featuresScaled")
        // This pipeline calculates the normalized features
        val featuresCalculatorModel = new Pipeline().setStages(Array(termCauseIndexer, termCauseEncoder, multiplier)).fit(df)
        // DF with features
        val vectorSum = new VectorSum
        val df_withFeatures = featuresCalculatorModel.transform(df).groupBy($"aggField1").agg(vectorSum($"featuresScaled").alias("features"), min($"sumPerBras")).cache

        df_withFeatures.orderBy("aggField1").show(30, false)

        if(df_withFeatures.count > 1) { // To avoid exception
          val kModel = new Pipeline().setStages(Array(new KMeans().setK(2).setSeed(1L))).fit(df_withFeatures)

          val predictions = kModel.transform(df_withFeatures)

          predictions.show(30, false)

          // Cluster centers
          val termCauseCenter = kModel.stages(0).asInstanceOf[KMeansModel].clusterCenters
          println(termCauseCenter.mkString)

          // UDF to calculate the distance from the center, taken from the "centersBRAS" array
          import org.apache.spark.ml.linalg.{Vector, Vectors}
          val distFromCenter = udf((tCauseVector: Vector, c: Int) => Vectors.sqdist(tCauseVector, termCauseCenter(c)))

          val decoratedPredictions = predictions.withColumn("distanceFromCenter", distFromCenter($"features", $"prediction")).cache

          // Get outliers, which are the points whose distance to center is higher than N x standard deviations
          val distanceStats = decoratedPredictions.describe("distanceFromCenter").cache
          val distanceStdDev = distanceStats.where($"summary" === "stddev").first.getString(1).toFloat
          val distanceMean = distanceStats.where($"summary" === "mean").first.getString(1).toFloat

          decoratedPredictions.where($"distanceFromCenter" > distanceMean + 2 * distanceStdDev).show(100, false)
        }

      }).start()

    strQuery.awaitTermination()
  }
}

// Template creation
// http://localhost:9200/_template/agg_cdr
/**
{
  "index_patterns": [
  "agg_cdr*",
  "agg2_cdr*"
  ],
  "settings": {
  "number_of_shards": 1,
  "number_of_replicas": 0
},
  "mappings": {
  "_source": {
  "enabled": true
},
  "properties": {
  "metricName": {
  "type": "keyword"
},
  "timestamp": {
  "type": "date"
},
  "date": {
  "type": "date"
},
  "count": {
  "type": "long"
},
  "value": {
  "type": "long"
},
  "samples": {
  "type": "long"
},
  "avg": {
  "type": "double"
},
  "stdDev": {
  "type": "double"
},
  "nasIpAddress": {
  "type": "keyword"
},
  "accessNode": {
  "type": "keyword"
},
  "isShort":{
  "type": "boolean"
},
  "acctTerminateCause":{
  "type": "keyword"
},
  "accessType":{
  "type": "keyword"
},
  "aggField1":{
  "type": "keyword"
},
  "aggField2":{
  "type": "keyword"
},
  "aggField3":{
  "type": "keyword"
},
  "aggField4":{
  "type": "keyword"
}
}

}
}
*/

