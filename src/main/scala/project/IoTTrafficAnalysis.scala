package project

import org.apache.spark.{SparkConf, SparkContext}
import java.text.SimpleDateFormat
import java.util.Date

object IoTTrafficAnalysis {

  case class TrafficRecord(
                            ts: Double,
                            uid: String,
                            id_orig_h: String,
                            id_orig_p: Int,
                            id_resp_h: String,
                            id_resp_p: Int,
                            proto: String,
                            service: String,
                            duration: Double,
                            orig_bytes: Long,
                            resp_bytes: Long,
                            conn_state: String,
                            local_orig: String,
                            local_resp: String,
                            missed_bytes: Long,
                            history: String,
                            orig_pkts: Int,
                            orig_ip_bytes: Long,
                            resp_pkts: Int,
                            resp_ip_bytes: Long,
                            tunnel_parents: String,
                            label: String,
                            timestamp: String,
                            date: String,
                            hour: Int
                          )

  case class IPProfile(
                        id_orig_h: String,
                        avg_bytes_sent: Double,
                        total_bytes_sent: Long,
                        connection_count: Long,
                        avg_duration: Double,
                        traffic_class: String
                      )

  case class EnrichedRecord(
                             record: TrafficRecord,
                             profile: IPProfile
                           )

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("IoT Traffic Analysis")
      .setMaster("local[*]")
      .set("spark.driver.memory", "6g")
      .set("spark.executor.memory", "6g")
      .set("spark.driver.extraJavaOptions", "-Xmx6g -Xms4g")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    println("=== IoT Traffic Analysis ===")
    val rawData = sc.textFile("C:\\Users\\muham\\Desktop\\Coding\\Unibo\\BigData\\BigDataProject\\datasets\\dataset23-2.csv")

    val header = rawData.first()
    val dataRDD = rawData
      .filter(line => line != header)
      .map(parseLine)
      .filter(_.isDefined)
      .map(_.get)

    dataRDD.cache()
    println(s"\nTotal records loaded: ${dataRDD.count()}")

    // FIRST SHUFFLE: Aggregate by source IP to calculate traffic profile
    val ipProfileRDD = dataRDD
      .map(record => (record.id_orig_h, (record.orig_bytes, record.duration, 1L)))
      .reduceByKey { case ((bytes1, dur1, count1), (bytes2, dur2, count2)) =>
        (bytes1 + bytes2, dur1 + dur2, count1 + count2)
      }
      .map { case (ip, (totalBytes, totalDur, count)) =>
        val avgBytes = totalBytes.toDouble / count
        val avgDur = totalDur / count
        val trafficClass = classifyTraffic(count, avgBytes)
        (ip, IPProfile(ip, avgBytes, totalBytes, count, avgDur, trafficClass))
      }

    // Cache IP profiles for reuse
    ipProfileRDD.cache()

    println("\n--- IP Traffic Classification ---")
    ipProfileRDD
      .take(20)
      .foreach { case (ip, profile) =>
        println(f"IP: $ip%-15s | Class: ${profile.traffic_class}%-30s | Connections: ${profile.connection_count}%5d | Avg Bytes: ${profile.avg_bytes_sent}%10.2f")
      }

    // SECOND SHUFFLE: Join back with original dataset
    val trafficWithIP = dataRDD.map(r => (r.id_orig_h, r))
    val enrichedRDD = trafficWithIP
      .leftOuterJoin(ipProfileRDD)
      .map { case (ip, (record, profileOpt)) =>
        val profile = profileOpt.getOrElse(
          IPProfile(ip, 0.0, 0L, 0L, 0.0, "Unknown")
        )
        EnrichedRecord(record, profile)
      }

    enrichedRDD.cache()

    // THIRD SHUFFLE: Aggregate by traffic class, protocol, and label (NO hour)
    val trafficPatternRDD = enrichedRDD
      .map { enriched =>
        val key = (enriched.profile.traffic_class, enriched.record.proto, enriched.record.label)
        val value = (1L, enriched.record.orig_bytes, enriched.record.duration, Set(enriched.record.id_resp_h))
        (key, value)
      }
      .reduceByKey { case ((count1, bytes1, dur1, dests1), (count2, bytes2, dur2, dests2)) =>
        (count1 + count2, bytes1 + bytes2, dur1 + dur2, dests1 ++ dests2)
      }
      .map { case ((trafficClass, proto, label), (count, totalBytes, totalDur, destinations)) =>
        val avgDur = if (count != 0) totalDur / count else 0.0
        (trafficClass, proto, label, count, totalBytes, avgDur, destinations.size)
      }
      .sortBy(t => (t._1, t._2, t._3))

    println("\n--- Traffic Patterns by IP Class and Protocol ---")
    println(f"${"Traffic Class"}%-30s | ${"Proto"}%-5s | ${"Label"}%-10s | ${"Connections"}%-11s | ${"Total Bytes"}%-12s | ${"Avg Duration"}%-12s | ${"Unique Dests"}%-12s")
    println("-" * 140)
    trafficPatternRDD
      .take(50)
      .foreach { case (trafficClass, proto, label, count, totalBytes, avgDur, uniqueDests) =>
        println(f"$trafficClass%-30s | $proto%-5s | $label%-10s | $count%11d | $totalBytes%12d | $avgDur%12.2f | $uniqueDests%12d")
      }

    // Additional analysis: Detect anomalies
    val anomalyRDD = enrichedRDD
      .filter(e => e.profile.traffic_class == "High Frequency High Volume") // && e.record.label == "benign")
      .map { enriched =>
        val key = (enriched.record.id_orig_h, enriched.record.date)
        val value = (1L, enriched.record.orig_bytes, Set(enriched.record.id_resp_h))
        (key, value)
      }
      .reduceByKey { case ((count1, bytes1, targets1), (count2, bytes2, targets2)) =>
        (count1 + count2, bytes1 + bytes2, targets1 ++ targets2)
      }
      .filter { case (_, (dailyConns, _, uniqueTargets)) =>
        dailyConns > 100 || uniqueTargets.size > 20
      }
      .map { case ((ip, date), (dailyConns, dailyBytes, uniqueTargets)) =>
        (ip, date, dailyConns, dailyBytes, uniqueTargets.size)
      }

    println("\n--- Potential Anomalies (High Activity IPs) ---")
    println(f"${"IP Address"}%-15s | ${"Date"}%-10s | ${"Daily Connections"}%-17s | ${"Daily Bytes"}%-12s | ${"Unique Targets"}%-14s")
    println("-" * 90)
    anomalyRDD
      .take(20)
      .foreach { case (ip, date, dailyConns, dailyBytes, uniqueTargets) =>
        println(f"$ip%-15s | $date%-10s | $dailyConns%17d | $dailyBytes%12d | $uniqueTargets%14d")
      }

    // ----- Comparison of Benign and Malicious Traffic Patterns (aggregated over hours) ----
    val maliciousPatternsRDD = computeTrafficPatterns(enrichedRDD, "malicious")
    val benignPatternsRDD = computeTrafficPatterns(enrichedRDD, "benign")

    val comparisonRDD = maliciousPatternsRDD
      .map { case (tc, proto, count, bytes, avgDur, uniqueDests) =>
        ((tc, proto), ("malicious", count, bytes, avgDur, uniqueDests))
      }
      .union(
        benignPatternsRDD.map { case (tc, proto, count, bytes, avgDur, uniqueDests) =>
          ((tc, proto), ("benign", count, bytes, avgDur, uniqueDests))
        }
      )
      .groupByKey()
      .map { case ((tc, proto), metrics) =>
        val maliciousMetrics = metrics.find(_._1 == "malicious").getOrElse(("malicious", 0L, 0L, 0.0, 0))
        val benignMetrics    = metrics.find(_._1 == "benign").getOrElse(("benign", 0L, 0L, 0.0, 0))
        (tc, proto,
          benignMetrics._2, benignMetrics._3, benignMetrics._4, benignMetrics._5,
          maliciousMetrics._2, maliciousMetrics._3, maliciousMetrics._4, maliciousMetrics._5
        )
      }

    println("\n--- Benign vs Malicious Traffic Patterns (aggregated over all hours) ---")
    println(f"${"Class"}%-30s | ${"Proto"}%-5s | ${"Benign Conns"}%-12s | ${"Benign Bytes"}%-12s | ${"Benign AvgDur"}%-12s | ${"Benign Dests"}%-12s | ${"Malic Conns"}%-12s | ${"Malic Bytes"}%-12s | ${"Malic AvgDur"}%-12s | ${"Malic Dests"}%-12s")
    println("-" * 150)

    comparisonRDD.take(50).foreach { case (tc, proto, bConns, bBytes, bAvgDur, bDests, mConns, mBytes, mAvgDur, mDests) =>
      println(f"$tc%-30s | $proto%-5s | $bConns%12d | $bBytes%12d | $bAvgDur%12.2f | $bDests%12d | $mConns%12d | $mBytes%12d | $mAvgDur%12.2f | $mDests%12d")
    }

    // Overall Distribution
    println("--- Overall Label Distribution ---")
    val labelDistribution = dataRDD
      .map(r => (r.label, 1L))
      .reduceByKey(_ + _)
      .collect()
      .sortBy(-_._2)

    val total = labelDistribution.map(_._2).sum
    labelDistribution.foreach { case (label, count) =>
      val percentage = (count.toDouble / total) * 100
      println(f"$label%-15s: $count%8d connections ($percentage%5.2f%%)")
    }


    println("\n\n=== LABEL DISTRIBUTION BY TRAFFIC CATEGORY ===\n")

    // Aggregate by traffic class and label
    val labelByCategory = enrichedRDD
      .map(e => ((e.profile.traffic_class, e.record.label), 1L))
      .reduceByKey(_ + _)
      .map { case ((trafficClass, label), count) => (trafficClass, (label, count)) }
      .groupByKey()
      .sortByKey()
      .collect()

    println("--- Label Distribution by Traffic Category ---")
    println(f"${"Traffic Category"}%-30s | ${"Benign"}%-12s | ${"Malicious"}%-12s | ${"Total"}%-12s | ${"Benign %"}%-10s | ${"Malicious %"}%-12s")
    println("-" * 110)

    labelByCategory.foreach { case (trafficClass, labelCounts) =>
      val countsMap = labelCounts.toMap
      val benign = countsMap.getOrElse("benign", 0L)
      val malicious = countsMap.getOrElse("malicious", 0L)
      val totalClass = benign + malicious
      val benignPercent = if (totalClass > 0) (benign.toDouble / totalClass) * 100 else 0.0
      val maliciousPercent = if (totalClass > 0) (malicious.toDouble / totalClass) * 100 else 0.0
      println(f"$trafficClass%-30s | $benign%12d | $malicious%12d | $totalClass%12d | $benignPercent%9.2f%% | $maliciousPercent%11.2f%%")
    }

    // Additional detailed breakdown by traffic class, protocol, and label
    println("\n--- Detailed Label Distribution by Traffic Category and Protocol ---")
    val detailedLabelDistribution = enrichedRDD
      .map(e => ((e.profile.traffic_class, e.record.proto, e.record.label), 1L))
      .reduceByKey(_ + _)
      .map { case ((trafficClass, proto, label), count) => ((trafficClass, proto), (label, count)) }
      .groupByKey()
      .sortByKey()
      .collect()

    println(f"${"Traffic Category"}%-30s | ${"Proto"}%-5s | ${"Benign"}%-12s | ${"Malicious"}%-12s | ${"Total"}%-12s | ${"Malicious %"}%-12s")
    println("-" * 110)

    detailedLabelDistribution.foreach { case ((trafficClass, proto), labelCounts) =>
      val countsMap = labelCounts.toMap
      val benign = countsMap.getOrElse("benign", 0L)
      val malicious = countsMap.getOrElse("malicious", 0L)
      val total = benign + malicious
      val maliciousPercent = if (total > 0) (malicious.toDouble / total) * 100 else 0.0
      println(f"$trafficClass%-30s | $proto%-5s | $benign%12d | $malicious%12d | $total%12d | $maliciousPercent%11.2f%%")
    }

    // Statistical summary by category
    println("\n--- Statistical Summary by Traffic Category ---")
    val categoryStats = enrichedRDD
      .map { e =>
        val key = (e.profile.traffic_class, e.record.label)
        val value = (e.record.orig_bytes, e.record.duration, e.record.orig_pkts, 1L)
        (key, value)
      }
      .reduceByKey { case ((b1, d1, p1, c1), (b2, d2, p2, c2)) =>
        (b1 + b2, d1 + d2, p1 + p2, c1 + c2)
      }
      .map { case ((trafficClass, label), (totalBytes, totalDur, totalPkts, count)) =>
        ((trafficClass, label), totalBytes.toDouble / count, totalDur / count, totalPkts.toDouble / count, count)
      }
      .collect()
      .groupBy(_._1._1) // Group by traffic class

    println(f"${"Traffic Category"}%-30s | ${"Label"}%-10s | ${"Avg Bytes"}%-12s | ${"Avg Duration"}%-12s | ${"Avg Packets"}%-12s | ${"Count"}%-12s")
    println("-" * 120)

    categoryStats.toSeq.sortBy(_._1).foreach { case (trafficClass, records) =>
      records.foreach { case ((_, label), avgBytes, avgDur, avgPkts, count) =>
        println(f"$trafficClass%-30s | $label%-10s | $avgBytes%12.2f | $avgDur%12.6f | $avgPkts%12.2f | $count%12d")
      }
    }

    // Visualization helper: Category risk assessment
    println("\n--- Traffic Category Risk Assessment ---")
    println("(Categories sorted by malicious traffic percentage)")

    val riskAssessment = labelByCategory
      .map { case (trafficClass, labelCounts) =>
        val countsMap = labelCounts.toMap
        val benign = countsMap.getOrElse("benign", 0L)
        val malicious = countsMap.getOrElse("malicious", 0L)
        val total = benign + malicious
        val maliciousPercent = if (total > 0) (malicious.toDouble / total) * 100 else 0.0
        val riskLevel = if (maliciousPercent >= 50) "HIGH RISK"
        else if (maliciousPercent >= 20) "MEDIUM RISK"
        else if (maliciousPercent >= 5) "LOW RISK"
        else "VERY LOW RISK"
        (trafficClass, maliciousPercent, total, malicious, riskLevel)
      }
      .sortBy(-_._2) // Sort by malicious percentage descending

    println(f"${"Traffic Category"}%-30s | ${"Malicious %"}%-12s | ${"Total Conns"}%-12s | ${"Malicious Conns"}%-15s | ${"Risk Level"}%-15s")
    println("-" * 110)

    riskAssessment.foreach { case (trafficClass, maliciousPercent, total, malicious, riskLevel) =>
      println(f"$trafficClass%-30s | $maliciousPercent%11.2f%% | $total%12d | $malicious%15d | $riskLevel%-15s")
    }


    println("\n--- Writing results to files ---")
    // Write traffic patterns
    trafficPatternRDD
      .map { case (trafficClass, proto, label, count, totalBytes, avgDur, uniqueDests) =>
        s"$trafficClass,$proto,$label,$count,$totalBytes,$avgDur,$uniqueDests"
      }
      .coalesce(1)
      .saveAsTextFile(s"output/traffic_patterns_rdd")

    // Write IP classifications
    ipProfileRDD
      .map { case (_, profile) =>
        s"${profile.id_orig_h},${profile.avg_bytes_sent},${profile.total_bytes_sent},${profile.connection_count},${profile.avg_duration},${profile.traffic_class}"
      }
      .coalesce(1)
      .saveAsTextFile("output/ip_classifications_rdd")

    println("\n=== Analysis Complete ===")
    sc.stop()
  }

  // Helper function to parse CSV line
  def parseLine(line: String): Option[TrafficRecord] = {
    try {
      val fields = line.split(",", -1)
      if (fields.length < 22) return None

      val ts = parseDouble(fields(0))
      val timestamp = formatTimestamp(ts)
      val date = extractDate(timestamp)
      val hour = extractHour(timestamp)

      Some(TrafficRecord(
        ts = ts,
        uid = fields(1),
        id_orig_h = fields(2),
        id_orig_p = parseInt(fields(3)),
        id_resp_h = fields(4),
        id_resp_p = parseInt(fields(5)),
        proto = fields(6),
        service = fields(7),
        duration = parseDouble(fields(8)),
        orig_bytes = parseLong(fields(9)),
        resp_bytes = parseLong(fields(10)),
        conn_state = fields(11),
        local_orig = fields(12),
        local_resp = fields(13),
        missed_bytes = parseLong(fields(14)),
        history = fields(15),
        orig_pkts = parseInt(fields(16)),
        orig_ip_bytes = parseLong(fields(17)),
        resp_pkts = parseInt(fields(18)),
        resp_ip_bytes = parseLong(fields(19)),
        tunnel_parents = fields(20),
        label = fields(21),
        timestamp = timestamp,
        date = date,
        hour = hour
      ))
    } catch {
      case _: Exception => None
    }
  }

  // Helper functions for parsing
  def parseDouble(s: String): Double = {
    try { s.toDouble } catch { case _: Exception => 0.0 }
  }

  def parseLong(s: String): Long = {
    try { s.toLong } catch { case _: Exception => 0L }
  }

  def parseInt(s: String): Int = {
    try { s.toInt } catch { case _: Exception => 0 }
  }

  def formatTimestamp(ts: Double): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    sdf.format(new Date((ts * 1000).toLong))
  }

  def extractDate(timestamp: String): String = {
    timestamp.split(" ")(0)
  }

  def extractHour(timestamp: String): Int = {
    try {
      timestamp.split(" ")(1).split(":")(0).toInt
    } catch {
      case _: Exception => 0
    }
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

  // Helper function to compute traffic patterns by label
  def computeTrafficPatterns(enrichedRDD: org.apache.spark.rdd.RDD[EnrichedRecord], label: String) = {
    enrichedRDD
      .filter(_.record.label == label)
      .map { enriched =>
        val key = (enriched.profile.traffic_class, enriched.record.proto)
        val value = (1L, enriched.record.orig_bytes, enriched.record.duration, Set(enriched.record.id_resp_h))
        (key, value)
      }
      .reduceByKey { case ((count1, bytes1, dur1, dests1), (count2, bytes2, dur2, dests2)) =>
        (count1 + count2, bytes1 + bytes2, dur1 + dur2, dests1 ++ dests2)
      }
      .map { case ((trafficClass, proto), (count, totalBytes, totalDur, destinations)) =>
        val avgDur = if (count != 0) totalDur / count else 0.0
        (trafficClass, proto, count, totalBytes, avgDur, destinations.size)
      }
  }
}