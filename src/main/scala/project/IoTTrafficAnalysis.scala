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
      .setAppName("IoT Traffic Analysis RDD")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    println("=== IoT Traffic Analysis with RDD Self-Join Pattern ===")
    val rawData = sc.textFile("C:\\Users\\muham\\Desktop\\Coding\\Unibo\\BigData\\BigDataProject\\datasets\\dataset1.csv")

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

    // THIRD SHUFFLE: Aggregate by traffic class, protocol, and hour
    val trafficPatternRDD = enrichedRDD
      .map { enriched =>
        val key = (enriched.profile.traffic_class, enriched.record.proto, enriched.record.hour, enriched.record.label)
        val value = (1L, enriched.record.orig_bytes, enriched.record.duration, Set(enriched.record.id_resp_h))
        (key, value)
      }
      .reduceByKey { case ((count1, bytes1, dur1, dests1), (count2, bytes2, dur2, dests2)) =>
        (count1 + count2, bytes1 + bytes2, dur1 + dur2, dests1 ++ dests2)
      }
      .map { case ((trafficClass, proto, hour, label), (count, totalBytes, totalDur, destinations)) =>
        val avgDur = totalDur / count
        (trafficClass, proto, hour, label, count, totalBytes, avgDur, destinations.size)
      }
      .sortBy(t => (t._1, t._2, t._3))

    println("\n--- Traffic Patterns by IP Class, Protocol, and Hour ---")
    println(f"${"Traffic Class"}%-30s | ${"Proto"}%-5s | ${"Hour"}%-4s | ${"Label"}%-10s | ${"Connections"}%-11s | ${"Total Bytes"}%-12s | ${"Avg Duration"}%-12s | ${"Unique Dests"}%-12s")
    println("-" * 150)
    trafficPatternRDD
      .take(50)
      .foreach { case (trafficClass, proto, hour, label, count, totalBytes, avgDur, uniqueDests) =>
        println(f"$trafficClass%-30s | $proto%-5s | $hour%4d | $label%-10s | $count%11d | $totalBytes%12d | $avgDur%12.2f | $uniqueDests%12d")
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

    println("\n--- Potential Anomalies (High Activity Benign IPs) ---")
    println(f"${"IP Address"}%-15s | ${"Date"}%-10s | ${"Daily Connections"}%-17s | ${"Daily Bytes"}%-12s | ${"Unique Targets"}%-14s")
    println("-" * 90)
    anomalyRDD
      .take(20)
      .foreach { case (ip, date, dailyConns, dailyBytes, uniqueTargets) =>
        println(f"$ip%-15s | $date%-10s | $dailyConns%17d | $dailyBytes%12d | $uniqueTargets%14d")
      }

    // ----- Comparison of Benign and Malicious Traffic Patterns ----
    val maliciousRDD = enrichedRDD.filter(_.record.label == "malicious")
    val maliciousPatternsRDD = maliciousRDD
      .map { enriched =>
        val key = (enriched.profile.traffic_class, enriched.record.proto, enriched.record.hour)
        val value = (1L, enriched.record.orig_bytes, enriched.record.duration, Set(enriched.record.id_resp_h))
        (key, value)
      }
      .reduceByKey { case ((count1, bytes1, dur1, dests1), (count2, bytes2, dur2, dests2)) =>
        (count1 + count2, bytes1 + bytes2, dur1 + dur2, dests1 ++ dests2)
      }
      .map { case ((trafficClass, proto, hour), (count, totalBytes, totalDur, destinations)) =>
        (trafficClass, proto, hour, count, totalBytes, totalDur / count, destinations.size)
      }

    val benignRDD = enrichedRDD.filter(_.record.label == "benign")
    val benignPatternsRDD = benignRDD
      .map { enriched =>
        val key = (enriched.profile.traffic_class, enriched.record.proto, enriched.record.hour)
        val value = (1L, enriched.record.orig_bytes, enriched.record.duration, Set(enriched.record.id_resp_h))
        (key, value)
      }
      .reduceByKey { case ((count1, bytes1, dur1, dests1), (count2, bytes2, dur2, dests2)) =>
        (count1 + count2, bytes1 + bytes2, dur1 + dur2, dests1 ++ dests2)
      }
      .map { case ((trafficClass, proto, hour), (count, totalBytes, totalDur, destinations)) =>
        (trafficClass, proto, hour, count, totalBytes, totalDur / count, destinations.size)
      }

    val comparisonRDD = maliciousPatternsRDD
      .map { case (tc, proto, hour, count, bytes, avgDur, uniqueDests) =>
        ((tc, proto, hour), ("malicious", count, bytes, avgDur, uniqueDests))
      }
      .union(
        benignPatternsRDD.map { case (tc, proto, hour, count, bytes, avgDur, uniqueDests) =>
          ((tc, proto, hour), ("benign", count, bytes, avgDur, uniqueDests))
        }
      )
      .groupByKey()  // Group both benign and malicious metrics per trafficClass-proto-hour
      .map { case ((tc, proto, hour), metrics) =>
        val maliciousMetrics = metrics.find(_._1 == "malicious").getOrElse(("malicious", 0L, 0L, 0.0, 0))
        val benignMetrics    = metrics.find(_._1 == "benign").getOrElse(("benign", 0L, 0L, 0.0, 0))
        (tc, proto, hour,
          benignMetrics._2, benignMetrics._3, benignMetrics._4, benignMetrics._5,
          maliciousMetrics._2, maliciousMetrics._3, maliciousMetrics._4, maliciousMetrics._5
        )
      }

    println("\n--- Benign vs Malicious Traffic Patterns ---")
    println(f"${"Class"}%-30s | ${"Proto"}%-5s | ${"Hour"}%-4s | ${"Benign Conns"}%-12s | ${"Benign Bytes"}%-12s | ${"Benign AvgDur"}%-12s | ${"Benign Dests"}%-12s | ${"Malic Conns"}%-12s | ${"Malic Bytes"}%-12s | ${"Malic AvgDur"}%-12s | ${"Malic Dests"}%-12s")
    println("-" * 150)

    comparisonRDD.take(50).foreach { case (tc, proto, hour, bConns, bBytes, bAvgDur, bDests, mConns, mBytes, mAvgDur, mDests) =>
      println(f"$tc%-30s | $proto%-5s | $hour%4d | $bConns%12d | $bBytes%12d | $bAvgDur%12.2f | $bDests%12d | $mConns%12d | $mBytes%12d | $mAvgDur%12.2f | $mDests%12d")
    }

    println("\n--- Writing results to files ---")

    // Write traffic patterns
    trafficPatternRDD
      .map { case (trafficClass, proto, hour, label, count, totalBytes, avgDur, uniqueDests) =>
        s"$trafficClass,$proto,$hour,$label,$count,$totalBytes,$avgDur,$uniqueDests"
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
}