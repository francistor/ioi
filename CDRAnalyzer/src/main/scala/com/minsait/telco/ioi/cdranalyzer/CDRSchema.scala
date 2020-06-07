package com.minsait.telco.ioi.cdranalyzer

import java.sql.Timestamp
import java.util.Date

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object StringImplicits {

  /**
   * Implicit conversion to class with methods that parse strings catching exceptions and:
   * - Return 0 for integers and longs
   * - Return empty string for String
   * @param s
   */
  implicit class StringImprovements(val s: String){
    import scala.util.control.Exception.catching

    def toIntSafe: Int = catching(classOf[NumberFormatException]).opt(s.toInt).getOrElse(0)
    def toLongSafe: Long = catching(classOf[NumberFormatException]).opt(s.toLong).getOrElse(0)
    def toTimestamp: java.sql.Timestamp =  Timestamp.valueOf(s.replaceAll("/", "-"))
    def mayBeEmpty: String = if(s.length == 0) "" else s
  }
}

import StringImplicits._

/**
 * Holds case classes for Metrics
 */
object FOAggs {
  def foAgg1FromString(s: String): FOAggregation1 = {
    val items = s.split(",")
    FOAggregation1(items(0), Timestamp.valueOf(items(1)), items(3).toLongSafe, items(2))
  }

  def foAgg2FromString(s: String): FOAggregation2 = {
    val items = s.split(",")
    FOAggregation2(items(0), Timestamp.valueOf(items(1)), items(4).toLongSafe, items(2), items(3))
  }

  def foAgg3FromString(s: String): FOAggregation3 = {
    val items = s.split(",")
    FOAggregation3(items(0), Timestamp.valueOf(items(1)), items(5).toLongSafe, items(2), items(3), items(4))
  }

  def foAgg4FromString(s: String): FOAggregation4 = {
    val items = s.split(",")
    FOAggregation4(items(0), Timestamp.valueOf(items(1)), items(6).toLongSafe, items(2), items(3), items(4), items(5))
  }
}

abstract class FOAggregation(aggName: String, timestamp: Timestamp, value: Long){
  def aggName: String
  def timestamp: Timestamp
  def value: Long
}
case class FOAggregation1(aggName: String, timestamp: Timestamp, value: Long, aggField1: String) extends FOAggregation(aggName, timestamp, value)
case class FOAggregation2(aggName: String, timestamp: Timestamp, value: Long, aggField1: String, aggField2: String) extends FOAggregation(aggName, timestamp, value)
case class FOAggregation3(aggName: String, timestamp: Timestamp, value: Long, aggField1: String, aggField2: String, aggField3: String) extends FOAggregation(aggName, timestamp, value)
case class FOAggregation4(aggName: String, timestamp: Timestamp, value: Long, aggField1: String, aggField2: String, aggField3: String, aggField4: String) extends FOAggregation(aggName, timestamp, value)

case class SOAggregation1Raw(window: (java.sql.Timestamp, java.sql.Timestamp), aggField1: String, values: Array[Long], sampleSize: Int)
case class SOAggregation1Stats(aggName: String, timestamp: java.sql.Timestamp, aggField1: String, mean: Double, stdDev: Double, samples: Int)

object CDR {
  def fromString(s: String): CDR = {
    val accessNodeADSLRegex = "(.+) .+".r
    val accessNodeFiberRegex = "(.+?)\\s.+".r

    import StringImplicits._

    val fields = s.split(",")

    val accessNodeADSL = fields(17) match {
      case accessNodeADSLRegex(accessNode) => Some(accessNode)
      case _ => None
    }

    val accessNodeFiber = fields(18) match {
      case accessNodeFiberRegex(accessNode) => Some(accessNode)
      case _ => None
    }

    val accessNode = accessNodeADSL.orElse(accessNodeFiber).getOrElse("<NONE>")

    CDR(
      Timestamp.valueOf(fields(0).replaceAll("/", "-")),  // Date
      fields(1).toLongSafe, // Timestamp
      fields(2), // Acct-Status-Type
      fields(3), // Acct-Session-Id
      fields(4).toLongSafe, // Acct-Session-Time
      fields(5).toLongSafe, // Acct-Output-Octets
      fields(6).toLongSafe, // Acct-Output-Gigawords
      fields(7).toLongSafe, // Acct-Input-Octets
      fields(8).toLongSafe, // Acct-Input-Gigawords
      fields(11), // NAS-Identifier
      fields(12), // NAS-IP-Address
      fields(13).toLongSafe, // NAS-Port
      fields(14).mayBeEmpty, // ClientId
      fields(15).mayBeEmpty, // AccessType
      fields(16).mayBeEmpty, // User-Name
      fields(17).mayBeEmpty, // Calling-Station-Id
      fields(18).mayBeEmpty, // DSLForum-Agent-Circuit-Id
      fields(19).mayBeEmpty, // DSLForum-Agent-Remote-Id
      fields(20).mayBeEmpty, // Framed-IP-Address
      fields(21).mayBeEmpty, // Connect-Info
      fields(22).mayBeEmpty, // Acct-Terminate-Cause
      fields(27).mayBeEmpty, // User-Mac
      accessNode,
      fields(0).substring(fields(0).indexOf(" "))
    )
  }
}

case class CDR(
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
                mac: String,
                // Synthetic
                accessNode: String,
                day: String
              )