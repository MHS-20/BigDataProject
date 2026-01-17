package project

import org.apache.spark.{SparkConf, SparkContext}
import project.IoTTrafficUtilities._
import utils.Commons._

object IoTTrafficAnalysisOptimized {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("IoT Traffic Analysis v2")
      .setMaster("local[*]")
      .set("spark.driver.memory", "10g")
      .set("spark.executor.memory", "10g")
      .set("spark.driver.extraJavaOptions", "-Xmx8g -Xms6g")

    val sc = new SparkContext(conf)
    initializeSparkContext("remote", sc)
    sc.setLogLevel("ERROR")

    println("=== IoT Traffic Analysis (Optimized) ===")
    val rawData = sc.textFile(getDatasetPath(args{0}, args{1}))
    //val rawData = sc.textFile("C:\\Users\\muham\\Desktop\\Coding\\Unibo\\Corsi\\BigData\\BigDataProject\\datasets\\dataset52.csv")

    val header = rawData.first()
    val dataRDD = rawData
      .filter(line => line != header)
      .map(parseLine)
      .filter(_.isDefined)
      .map(_.get)

    dataRDD.cache()
    println(s"\nTotal records loaded: ${dataRDD.count()}")

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
    printIPProfilesEnriched(ipProfileRDD.take(20))

    // crea una riga per ogni coppia (ip, security label)
    val perCategoryRDD = ipProfileRDD.flatMap { case (_, profile) =>
      Seq(((profile.traffic_class, "benign"), (profile.total_bytes_sent, profile.avg_duration * profile.connection_count, profile.total_packets, profile.benign_count)),
        ((profile.traffic_class, "malicious"), (profile.total_bytes_sent, profile.avg_duration * profile.connection_count, profile.total_packets, profile.malicious_count)))
    }

    // SECOND SHUFFLE: Statistical summary by ip profile
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

    printCategoryStats(categoryStats.collect())
   // saveResultsOptimized(sc, categoryStats, ipProfileRDD)
    println("\n=== Analysis Complete ===")
    sc.stop()
  }
}