package project.optv2

import org.apache.spark.{SparkConf, SparkContext}
import project.IoTTrafficUtilities._
import project.{CategoryStats, TrafficClassStats}

object IoTTrafficAnalysisv2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("IoT Traffic Analysis v2")
      .setMaster("local[*]")
      .set("spark.driver.memory", "6g")
      .set("spark.executor.memory", "6g")
      .set("spark.driver.extraJavaOptions", "-Xmx6g -Xms4g")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    println("=== IoT Traffic Analysis v2 (Optimized) ===")
    val rawData = sc.textFile("C:\\Users\\muham\\Desktop\\Coding\\Unibo\\Corsi\\BigData\\BigDataProject\\datasets\\dataset52.csv")

    val header = rawData.first()
    val dataRDD = rawData
      .filter(line => line != header)
      .map(parseLine)
      .filter(_.isDefined)
      .map(_.get)

    dataRDD.cache()
    println(s"\nTotal records loaded: ${dataRDD.count()}")

    // FIRST SHUFFLE: Aggregate by source IP with label distribution
    val ipProfileRDD = dataRDD
      .map { record =>
        val benignCount = if (record.label == "benign") 1L else 0L
        val maliciousCount = if (record.label == "malicious") 1L else 0L
        (record.id_orig_h, (record.orig_bytes, record.duration, 1L, benignCount, maliciousCount))
      }
      .reduceByKey { case ((bytes1, dur1, count1, benign1, mal1), (bytes2, dur2, count2, benign2, mal2)) =>
        (bytes1 + bytes2, dur1 + dur2, count1 + count2, benign1 + benign2, mal1 + mal2)
      }
      .map { case (ip, (totalBytes, totalDur, count, benignCount, maliciousCount)) =>
        val avgBytes = totalBytes.toDouble / count
        val avgDur = totalDur / count
        val trafficClass = classifyTraffic(count, avgBytes)
        val benignPercent = (benignCount.toDouble / count) * 100
        val maliciousPercent = (maliciousCount.toDouble / count) * 100

        (ip, IPProfileEnriched(
          ip,
          avgBytes,
          totalBytes,
          count,
          avgDur,
          trafficClass,
          benignCount,
          maliciousCount,
          benignPercent,
          maliciousPercent
        ))
      }

    ipProfileRDD.cache()
    printIPProfilesEnriched(ipProfileRDD.take(20))

    // SECOND SHUFFLE (CON JOIN): Statistical summary by category and label
    // TODO: in teoria dovresti poterlo fare senza il join
//    val categoryStats = dataRDD
//      .map(record => (record.id_orig_h, record))
//      .join(ipProfileRDD)
//      .map { case (ip, (record, profile)) =>
//        ((profile.traffic_class, record.label), (record.orig_bytes, record.duration, record.orig_pkts, 1L))
//      }
//      .reduceByKey { case ((b1, d1, p1, c1), (b2, d2, p2, c2)) =>
//        (b1 + b2, d1 + d2, p1 + p2, c1 + c2)
//      }
//      .map { case ((tc, lbl), (totB, totD, totP, cnt)) =>
//        CategoryStats(tc, lbl, totB.toDouble / cnt, totD / cnt, totP.toDouble / cnt, cnt)
//      }
//      // .sortBy(cs => (cs.traffic_class, cs.label))

    // --- SECOND SHUFFLE (UNICO) ---
    // Statistical summary by category and label senza join
    val categoryStats = ipProfileRDD
      .flatMap { case (_, profile) =>
        // Per ogni IP generiamo una entry per ciascun label presente
        Seq(
          ("benign", profile.benign_count, profile.avg_bytes_sent, profile.avg_duration, profile.connection_count),
          ("malicious", profile.malicious_count, profile.avg_bytes_sent, profile.avg_duration, profile.connection_count)
        )
          .filter(_._2 > 0) // ignora label con count = 0
          .map { case (label, count, avgBytes, avgDur, totalConns) =>
            ((profile.traffic_class, label), (avgBytes * count, avgDur * count, count))
          }
      }
      .combineByKey(
        // createCombiner
        (v: (Double, Double, Long)) => v,
        // mergeValue (all’interno della stessa partizione)
        (acc: (Double, Double, Long), v: (Double, Double, Long)) =>
          (acc._1 + v._1, acc._2 + v._2, acc._3 + v._3),
        // mergeCombiners (tra partizioni)
        (acc1: (Double, Double, Long), acc2: (Double, Double, Long)) =>
          (acc1._1 + acc2._1, acc1._2 + acc2._2, acc1._3 + acc2._3)
      )
      .map { case ((trafficClass, label), (totBytes, totDur, totCount)) =>
        CategoryStats(
          trafficClass,
          label,
          totBytes / totCount,
          totDur / totCount,
          0.0,   // avg_packets: se serve, puoi aggiungerlo nello stesso modo
          totCount
        )
      }

    printCategoryStats(categoryStats.collect())

    saveResults(sc, categoryStats, ipProfileRDD)

    println("\n=== Analysis Complete ===")
    sc.stop()
  }

  def classifyTraffic(connectionCount: Long, avgBytes: Double): String = {
    if (connectionCount < 10) {
      "Low Activity"
    } else if (connectionCount >= 10 && connectionCount < 50) {
      "Normal Activity"
    } else if (connectionCount >= 50 && avgBytes < 1000) {
      "High Frequency Low Volume"
    } else if (connectionCount >= 50 && avgBytes >= 1000) {
      "High Frequency High Volume"
    } else {
      "Unknown"
    }
  }

  def printIPProfilesEnriched(profiles: Array[(String, IPProfileEnriched)]): Unit = {
    println("\n--- IP Traffic Classification ---")
    profiles.foreach { case (ip, profile) =>
      println(
        f"IP: $ip%-15s | " +
          f"Class: ${profile.traffic_class}%-30s | " +
          f"Connections: ${profile.connection_count}%5d | " +
          f"Avg Bytes: ${profile.avg_bytes_sent}%10.2f | " +
          f"Benign %%: ${profile.benign_percent}%6.2f%%%% | " +
          f"Malicious %%: ${profile.malicious_percent}%6.2f%%%%"
      )
    }
  }


  def saveResults(sc: SparkContext,
                  categoryStats: org.apache.spark.rdd.RDD[CategoryStats],
                  ipProfiles: org.apache.spark.rdd.RDD[(String, IPProfileEnriched)]): Unit = {
    val outputPath = "C:\\Users\\muham\\Desktop\\Coding\\Unibo\\Corsi\\BigData\\BigDataProject\\output"

    try {
      categoryStats
        .map(s => s"${s.traffic_class},${s.label},${s.count},${s.avg_duration},${s.avg_bytes},${s.avg_packets}")
        .saveAsTextFile(s"$outputPath/traffic_class_stats")

      ipProfiles
        .map { case (ip, p) =>
          s"${p.id_orig_h},${p.avg_bytes_sent},${p.total_bytes_sent},${p.connection_count}," +
            s"${p.avg_duration},${p.traffic_class},${p.benign_count},${p.malicious_count}," +
            s"${p.benign_percent},${p.malicious_percent}"
        }
        .saveAsTextFile(s"$outputPath/ip_profiles_enriched")

      println(s"\nResults saved to: $outputPath")
    } catch {
      case e: Exception => println(s"Warning: Could not save results - ${e.getMessage}")
    }
  }
}