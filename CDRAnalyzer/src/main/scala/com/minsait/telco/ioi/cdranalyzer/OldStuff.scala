package com.minsait.telco.ioi.cdranalyzer

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}

object CDRSchema {

  import StringImplicits._

  val schema: StructType = StructType(Seq(
    StructField("Date", TimestampType, nullable = false),
    StructField("Timestamp", LongType, nullable = false),
    StructField("Acct-Status-Type", StringType, nullable = false),
    StructField("Acct-Session-Id", StringType, nullable = false),
    StructField("Acct-Session-Time", StringType, nullable = false),
    StructField("Acct-Output-Octets", LongType, nullable = true),
    StructField("Acct-Output-Gigawords", LongType, nullable = true),
    StructField("Acct-Input-Octets", LongType, nullable = true),
    StructField("Acct-Input-Gigawords", LongType, nullable = true),
    StructField("Acct-Output-Packets", LongType, nullable = true),
    StructField("Acct-Input-Packets", LongType, nullable = true),
    StructField("NAS-Identifier", StringType, nullable = false),
    StructField("NAS-IP-Address", StringType, nullable = false),
    StructField("NAS-Port", LongType, nullable = false),
    StructField("ClientId", StringType, nullable = true),
    StructField("AccessType", IntegerType, nullable = true),
    StructField("User-Name", StringType, nullable = true),
    StructField("Calling-Station-Id", StringType, nullable = true),
    StructField("DSLForum-Agent-Circuit-Id", StringType, nullable = true),
    StructField("DSLForum-Agent-Remote-Id", StringType, nullable = true),
    StructField("Framed-IP-Address", StringType, nullable = true),
    StructField("Connect-Info", StringType, nullable = true),
    StructField("Acct-Terminate-Cause", StringType, nullable = true),
    StructField("NAT-Start-Port", IntegerType, nullable = true),
    StructField("NAT-End-Port", IntegerType, nullable = true),
    StructField("Acct-Delay-Time", IntegerType, nullable = true),
    StructField("NAT-IP-Address", StringType, nullable = true),
    StructField("User-Mac", StringType, nullable = true)
    /*
  StructField("Chargeable-User-Identity", StringType, nullable = true),
  StructField("Delegated-IPv6-Prefix", StringType, nullable = true),
  StructField("Unisphere-Ipv6-Acct-Input-Packets", LongType, nullable = true),
  StructField("Unisphere-Ipv6-Acct-Output-Packets", LongType, nullable = true),
  StructField("Unisphere-Ipv6-Acct-Input-Octets", LongType, nullable = true),
  StructField("Unisphere-Ipv6-Acct-Output-Octets", LongType, nullable = true)

     */
  ))

  def stringToCDR(row: String): Row = {
    val fields = row.split(",")
    try {
      Row(
        fields(0).toTimestamp, // Date
        fields(1).toLongSafe, // Timestamp
        fields(2), // Acct-Status-Type
        fields(3), // Acct-Session-Id
        fields(4).toLongSafe, // Acct-Session-Time
        fields(5).toLongSafe, // Acct-Output-Octets
        fields(6).toLongSafe, // Acct-Output-Gigawords
        fields(7).toLongSafe, // Acct-Input-Octets
        fields(8).toLongSafe, // Acct-Input-Gigawords
        fields(9).toLongSafe, // Acct-Output-Packets
        fields(10).toLongSafe, // Acct-Input-Packets
        fields(11), // NAS-Identifier
        fields(12), // NAS-IP-Address
        fields(13).toLongSafe, // NAS-Port
        fields(14).mayBeEmpty, // ClientId
        fields(15).toIntSafe, // AccessType
        fields(16).mayBeEmpty, // User-Name
        fields(17).mayBeEmpty, // Calling-Station-Id
        fields(18).mayBeEmpty, // DSLForum-Agent-Circuit-Id
        fields(19).mayBeEmpty, // DSLForum-Agent-Remote-Id
        fields(20).mayBeEmpty, // Framed-IP-Address
        fields(21).mayBeEmpty, // Connect-Info
        fields(22).mayBeEmpty, // Acct-Terminate-Cause
        fields(23).toIntSafe, // NAT-Start-Port
        fields(24).toIntSafe, // NAT-End-Port
        fields(25).toIntSafe, // Acct-Delay-Time
        fields(26).mayBeEmpty, // NAT-IP-Address
        fields(27).mayBeEmpty  // User-Mac
        /*
        fields(28).mayBeEmpty, // Chargeable-User-Identity
        fields(29).mayBeEmpty, // Delegated-IPv6-Prefix
        fields(30).toLongSafe, // Unisphere-Ipv6-Acct-Input-Packets
        fields(31).toLongSafe, // Unisphere-Ipv6-Acct-Output-Packets
        fields(32).toLongSafe, // Unisphere-Ipv6-Acct-Input-Octets
        fields(33).toLongSafe, // Unisphere-Ipv6-Acct-Output-Octets
         */
      )
    }
    catch{
      case e: Exception => println(row); throw e
    }

  }

}

case class CDR2(
                date: java.sql.Timestamp,
                timestamp: Long,
                acctStatusType: String,
                acctSessionId: String,
                sessionTime: Long,
                outputOctets: Long,
                outputGigawords: Long,
                inputOctets: Long,
                inputGigawords: Long,
                nasIdentifier: String,
                nasIpAddress: String,
                nasPort: Long,
                clientId: String,
                accessType: String,
                userName: String,
                callingStationId: String,
                circuitId: String,
                remoteId: String,
                framedIPAddress: String,
                connectInfo: String,
                terminateCause: String,
                mac: String
              ){



  def toTuple: (java.sql.Timestamp, Long, String, String, Long, Long, Long, Long, Long, String, String, Long, String, String, String, String, String, String, String, String, String, String) = (
    date,               // 1
    timestamp,          // 2
    acctStatusType,     // 3
    acctSessionId,      // 4
    sessionTime,        // 5
    outputOctets,       // 6
    outputGigawords,    // 7
    inputOctets,        // 8
    inputGigawords,     // 9
    nasIdentifier,      // 10
    nasIpAddress,       // 11
    nasPort,            // 12
    clientId,           // 13
    accessType,         // 14
    userName,           // 15
    callingStationId,   // 16
    circuitId,          // 17
    remoteId,           // 18
    framedIPAddress,    // 19
    connectInfo,        // 20
    terminateCause,     // 21
    mac                 // 22
  )
}

/*
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

        val cdrStream = cdrStringDataFrame.withColumn("CDRItems", toCDRItems($"value"))
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

    cdrStream.printSchema()
 */

/*
 /**
   * Version with classic Streaming
   */
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

      // Convert RDD[String] to DataFrame
      val cdrRDD = rdd.map(record => CDRSchema.stringToCDR(record.value))
      val cdrDF = session.createDataFrame(cdrRDD, CDRSchema.schema)
      println(cdrDF.count)


      // Create a temporary view
      wordsDataFrame.createOrReplaceTempView("words")

      // Do word count on DataFrame using SQL and print it
      val wordCountsDataFrame =
        spark.sql("select word, count(*) as total from words group by word")
      wordCountsDataFrame.show()


    }

    // val a = stream.map(record => (record.key, record.value))

    ssc.start()
    ssc.awaitTermination()
  }
 */
