package project

import org.apache.spark.{SparkConf, SparkContext}
import project.IoTTrafficUtilities._
import utils.Commons._
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._

object IoTTrafficAnalysisOptimized {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("IoT Traffic Analysis v2")
    //.set("spark.executor.instances", "2")
    //.set("spark.executor.cores", "4")
    //.set("spark.executor.memory", "10g")
    //.set("spark.driver.memory", "4g")

    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    initializeSparkContext("remote", sc)
    sc.setLogLevel("ERROR")

    println("=== IoT Traffic Analysis (Optimized) ===")
    val rawData = sc.textFile(getDatasetPath(args(0), args(1)))

    val header = rawData.first()
    val dataRDD = rawData
      .filter(line => line != header)
      .map(parseLine)
      .filter(_.isDefined)
      .map(_.get)

    dataRDD.cache()

    // FIRST SHUFFLE: Aggregate by source IP with label distribution (counting malicious and benign)
    val ipProfileRDD = dataRDD
      .map { record =>
        val benignCount = if (record.label == "benign") 1L else 0L
        val maliciousCount = if (record.label == "malicious") 1L else 0L
        (record.id_orig_h, (record.orig_bytes, record.duration, 1L, benignCount, maliciousCount, record.orig_pkts))
      }
      .reduceByKey { case ((bytes1, dur1, count1, benign1, mal1, pkts1), (bytes2, dur2, count2, benign2, mal2, pkts2)) =>
        (bytes1 + bytes2, dur1 + dur2, count1 + count2, benign1 + benign2, mal1 + mal2, pkts1 + pkts2)
      }
      .map { case (ip, (totalBytes, totalDur, count, benignCount, maliciousCount, totalPkts)) =>
        val avgBytes = totalBytes.toDouble / count
        val avgDur = totalDur / count
        val trafficClass = classifyTraffic(count, avgBytes)
        val benignPercent = (benignCount.toDouble / count) * 100
        val maliciousPercent = (maliciousCount.toDouble / count) * 100
        (ip, IPProfileEnriched(ip, avgBytes, totalBytes, count, avgDur, totalPkts, trafficClass, benignCount, maliciousCount, benignPercent, maliciousPercent
        ))
      }

    ipProfileRDD.cache()

    // crea una riga per ogni coppia (ip, security label)
    val perCategoryRDD = ipProfileRDD.flatMap { case (_, profile) =>
      Seq(((profile.traffic_class, "benign"), (profile.total_bytes_sent, profile.avg_duration * profile.connection_count, profile.total_packets, profile.benign_count)),
        ((profile.traffic_class, "malicious"), (profile.total_bytes_sent, profile.avg_duration * profile.connection_count, profile.total_packets, profile.malicious_count)))
    }

    // SECOND SHUFFLE: Statistical summary by traffc profile
    val categoryStats = perCategoryRDD
      .reduceByKey { case ((b1, d1, p1, c1), (b2, d2, p2, c2)) => (b1 + b2, d1 + d2, p1 + p2, c1 + c2) }
      .map { case ((trafficClass, label), (totBytes, totDur, totPkts, count)) =>
        val safeCount = if (count == 0) 1 else count
        CategoryStats(
          trafficClass,
          label,
          avg_bytes    = totBytes.toDouble / safeCount,
          avg_duration = totDur / safeCount,
          avg_packets  = totPkts.toDouble / safeCount,
          count        = count
        )
      }

    import spark.implicits._
    val categoryDF = categoryStats.toDF()
    categoryDF
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv("s3a://mhs-lab1/output/v2/categoryStats/")

    val ipProfileDF = ipProfileRDD.toDF()
    ipProfileDF
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv("s3a://mhs-lab1/output/v2/ipProfiles/")

    println("\n=== Analysis Complete ===")
    spark.stop()
    sc.stop()
  }
}