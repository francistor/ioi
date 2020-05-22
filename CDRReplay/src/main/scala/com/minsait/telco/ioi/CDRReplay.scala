// Indra OSS Insights

package com.minsait.telco.ioi

import java.text.SimpleDateFormat
import java.util.Properties

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source

import org.apache.kafka.clients.producer._

object CDRReplay extends App{

  case class DatedCDR(date: java.util.Date, cdrFields: Array[String])

  val argsMap = collection.mutable.Map[String, String]()

  for (i <- args.indices){
    if(args(i) == "--help" || args(i) == "-?") argsMap("help") = "true"
    if(args(i) == "--dir") argsMap("originFolder") = args(i + 1)
    if(args(i) == "--date") argsMap("startDate") = args(i + 1)
  }

  if(argsMap.contains("help") || !argsMap.contains("originFolder") || !argsMap.contains("startDate")){
    println("Usage: CDRReplay.sh --dir <base directory> --date <yyyy-MM-ddTHH-mm>")
    System.exit(0)
  }

  val originFolder = argsMap("originFolder")
  println(s"Reading from $originFolder")

  val startDate = argsMap("startDate")
  println(s"Start date $startDate")

  val fileDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH-mm")
  val cdrDateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")

  val startTimestamp = fileDateFormat.parse(startDate).getTime
  val offsetTimestamp = System.currentTimeMillis() - startTimestamp

  val originFile = new java.io.File(originFolder)
  val leafs = originFile.listFiles(_.isDirectory).toList.map(file => file.getName)

  // Initialize Kafka
  val kafkaProperties = new Properties()
  kafkaProperties.put("bootstrap.servers", "localhost:9092")
  kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val producer = new KafkaProducer[String, String](kafkaProperties)

  val tasks = leafs.map{leaf => Future{dirLoop(s"$originFolder/$leaf")}}

  Await.result(Future.reduceLeft(tasks)((_,_) => ()), 1000.seconds)

  producer.close()

  def dirLoop(folderName: String): Unit = {
    var consecutiveMissingFiles = 0
    try {
      for (i <- 0 to 1) {
        val fileTimestamp = startTimestamp + i * 1000 * 60
        val currentFileName = s"$folderName/cdr.${fileDateFormat.format(new java.util.Date(fileTimestamp))}.txt"
        if (new java.io.File(currentFileName).isFile) {
          consecutiveMissingFiles = 0
          val file = Source.fromFile(currentFileName)
          file.getLines().map(cdr => {
            val cdrFields = cdr.split(",")
            DatedCDR(cdrDateFormat.parse(cdrFields(0)), cdrFields)
          }).foreach(datedCDR => {
            // Wait if necessary
            while (datedCDR.date.getTime > System.currentTimeMillis - offsetTimestamp) Thread.sleep(100)
            pushCDR(datedCDR)
          })
          file.close
        } else {
          println(s"[WARNING] $currentFileName does not exist")
          consecutiveMissingFiles = consecutiveMissingFiles + 1
          if (consecutiveMissingFiles > 20) return
          Thread.sleep(1000)
        }
      }
    } catch {
      case e: Exception => print("Exception ", e.getStackTrace)
    }
  }

  def pushCDR(datedCDR: DatedCDR): Unit = {
    val cdrFields = datedCDR.cdrFields

    // Key is NAS-IP-Address plus AcctSessionId plus duration
    val record = new ProducerRecord[String, String]("cdr", cdrFields(12) + "-" + cdrFields(3) + "-" + cdrFields(4), datedCDR.cdrFields.mkString(","))

    producer.send(record)
    println(datedCDR.cdrFields.mkString(","))
  }

}
