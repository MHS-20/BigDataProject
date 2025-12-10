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

  case class TrafficClassStats(
                                traffic_class: String,
                                benign: Long,
                                malicious: Long,
                                total: Long,
                                benign_percent: Double,
                                malicious_percent: Double
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
    val rawData = sc.textFile("C:\\Users\\muham\\Desktop\\Coding\\Unibo\\BigData\\BigDataProject\\datasets\\dataset52.csv")

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

    // THIRD SHUFFLE: Compute traffic patterns by label
    val labelByCategory = enrichedRDD
      .map(e => ((e.profile.traffic_class, e.record.label), 1L))
      .reduceByKey(_ + _)
      .map { case ((trafficClass, label), count) => (trafficClass, Map(label -> count)) }
      .reduceByKey(_ ++ _)
      .map { case (trafficClass, countsMap) => createTrafficClassStats(trafficClass, countsMap) }
      .sortBy(_.traffic_class)

    println("\n--- Label Distribution by Traffic Category ---")
    println(f"${"Traffic Category"}%-30s | ${"Benign"}%-12s | ${"Malicious"}%-12s | ${"Total"}%-12s | ${"Benign %"}%-10s | ${"Malicious %"}%-12s")
    println("-" * 110)

    labelByCategory
      .take(20)
      .foreach { stats =>
        println(f"${stats.traffic_class}%-30s | ${stats.benign}%12d | ${stats.malicious}%12d | ${stats.total}%12d | ${stats.benign_percent}%9.2f%% | ${stats.malicious_percent}%11.2f%%")
      }

    // Statistical summary by category
    println("\n--- Statistical Summary by Traffic Category ---")
    val categoryStats = enrichedRDD
      .map(e => ((e.profile.traffic_class, e.record.label), (e.record.orig_bytes, e.record.duration, e.record.orig_pkts, 1L)))
      .reduceByKey { case ((b1, d1, p1, c1), (b2, d2, p2, c2)) => (b1 + b2, d1 + d2, p1 + p2, c1 + c2) }
      .map { case ((tc, lbl), (totB, totD, totP, cnt)) =>
        ((tc, lbl), totB.toDouble / cnt, totD / cnt, totP.toDouble / cnt, cnt)}
      .collect()
      .groupBy(_._1._1)

    println(f"${"Traffic Category"}%-30s | ${"Label"}%-10s | ${"Avg Bytes"}%-12s | ${"Avg Duration"}%-12s | ${"Avg Packets"}%-12s | ${"Count"}%-12s")
    println("-" * 120)

    categoryStats.toSeq.sortBy(_._1).foreach { case (trafficClass, records) =>
      records.foreach { case ((_, label), avgBytes, avgDur, avgPkts, count) =>
        println(f"$trafficClass%-30s | $label%-10s | $avgBytes%12.2f | $avgDur%12.6f | $avgPkts%12.2f | $count%12d")
      }
    }

    printCategoryStats(enrichedRDD)
    saveResults(labelByCategory, ipProfileRDD)

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

  // -------------------- PRINT FUNCTIONS --------------------
  def printIPProfiles(profiles: Array[(String, IPProfile)]): Unit = {
    println("\n--- IP Traffic Classification ---")
    profiles.foreach { case (ip, profile) =>
      println(f"IP: $ip%-15s | Class: ${profile.traffic_class}%-30s | Connections: ${profile.connection_count}%5d | Avg Bytes: ${profile.avg_bytes_sent}%10.2f")
    }
  }

  def printLabelDistribution(stats: Array[TrafficClassStats]): Unit = {
    println("\n--- Label Distribution by Traffic Category ---")
    println(f"${"Traffic Category"}%-30s | ${"Benign"}%-12s | ${"Malicious"}%-12s | ${"Total"}%-12s | ${"Benign %"}%-10s | ${"Malicious %"}%-12s")
    println("-" * 110)
    stats.foreach { stat =>
      println(f"${stat.traffic_class}%-30s | ${stat.benign}%12d | ${stat.malicious}%12d | ${stat.total}%12d | ${stat.benign_percent}%9.2f%% | ${stat.malicious_percent}%11.2f%%")
    }
  }

  def printCategoryStats(categoryStats:) : Unit = {
    println("\n--- Statistical Summary by Traffic Category ---")
    println(f"${"Traffic Category"}%-30s | ${"Label"}%-10s | ${"Avg Bytes"}%-12s | ${"Avg Duration"}%-12s | ${"Avg Packets"}%-12s | ${"Count"}%-12s")
    println("-" * 120)
    categoryStats.toSeq.sortBy(_._1).foreach { case (trafficClass, records) =>
      records.foreach { case ((_, label), avgBytes, avgDur, avgPkts, count) =>
        println(f"$trafficClass%-30s | $label%-10s | $avgBytes%12.2f | $avgDur%12.6f | $avgPkts%12.2f | $count%12d")
      }
    }
  }

  // -------------------- UTILITY FUNCTIONS --------------------
  def saveResults(labelByCategory: org.apache.spark.rdd.RDD[TrafficClassStats],
                  ipProfileRDD: org.apache.spark.rdd.RDD[(String, IPProfile)]): Unit = {
    println("\n--- Writing results to files ---")
    labelByCategory
      .map { stats =>
        s"${stats.traffic_class},${stats.benign},${stats.malicious},${stats.total},${stats.benign_percent},${stats.malicious_percent}"
      }
      .coalesce(1)
      .saveAsTextFile("output/label_by_category_rdd")

    ipProfileRDD
      .map { case (_, profile) =>
        s"${profile.id_orig_h},${profile.avg_bytes_sent},${profile.total_bytes_sent},${profile.connection_count},${profile.avg_duration},${profile.traffic_class}"
      }
      .coalesce(1)
      .saveAsTextFile("output/ip_classifications_rdd")
  }

  def createTrafficClassStats(trafficClass: String, countsMap: Map[String, Long]): TrafficClassStats = {
    val benign = countsMap.getOrElse("benign", 0L)
    val malicious = countsMap.getOrElse("malicious", 0L)
    val total = benign + malicious
    val benignPercent = if (total > 0) (benign.toDouble / total) * 100 else 0.0
    val maliciousPercent = if (total > 0) (malicious.toDouble / total) * 100 else 0.0
    TrafficClassStats(trafficClass, benign, malicious, total, benignPercent, maliciousPercent)
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


}
