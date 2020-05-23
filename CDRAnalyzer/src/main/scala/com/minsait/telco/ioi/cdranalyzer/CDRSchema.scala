package com.minsait.telco.ioi.cdranalyzer

import java.sql.Timestamp

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object StringImplicits {
  implicit class StringImprovements(val s: String){
    import scala.util.control.Exception.catching

    def toIntSafe: Any = catching(classOf[NumberFormatException]).opt(s.toInt).orNull
    def toLongSafe: Any = catching(classOf[NumberFormatException]).opt(s.toLong).orNull
    def toTimestampSafe: Any =  catching(classOf[IllegalArgumentException]).opt(Timestamp.valueOf(s.replaceAll("/", "-"))).orNull
    def mayBeEmpty: Any = if(s.length == 0) null else s
  }
}

object CDRSchema {

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

  import StringImplicits._

  def stringToCDR(row: String): Row = {
    val fields = row.split(",")
    try {
      Row(
        fields(0).toTimestampSafe, // Date
        fields(1).toLongSafe, // Timestamp
        fields(2), // Acct-Status-Type
        fields(3), // Acct-Session-Id
        fields(4), // Acct-Session-Time
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
        fields(27).mayBeEmpty, // User-Mac
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
/*

2019/12/04 00:00:11,                                0
1575414011,                                         1
Alive,                                              2
CBR12-C0127329290017788a2e8AAAi7n,
64800,
8762771,
0,                                                  6
524297,                                             7
0,                                                  8
6926613,                                            9
3601581,                                            10
CBR12-CH-BCN,                                       11
212.230.100.112,                                    12
19173553,                                           13
n0810026#2_2929_177,                                14
4,
CBR12-CH-BCN-01273292900177@masmovil-ftth,
2929_177,
,                                                   18
,
100.80.4.66,                                        20
,
,
33792,                                              23
35839,
0,                                                  25
93.176.149.163,
4c:6e:6e:bc:a0:19,                                  27
n0810026#2,
,
0,
0,
0,
0

*/