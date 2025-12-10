package project

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext

import java.text.SimpleDateFormat
import java.util.Date

object IoTTrafficUtilities {
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

  def printCategoryStats(categoryStats: Array[CategoryStats]): Unit = {
    println("\n--- Statistical Summary by Traffic Category ---")
    println(f"${"Traffic Category"}%-30s | ${"Label"}%-10s | ${"Avg Bytes"}%-12s | ${"Avg Duration"}%-12s | ${"Avg Packets"}%-12s | ${"Count"}%-12s")
    println("-" * 120)
    categoryStats.foreach { cs =>
      println(f"${cs.traffic_class}%-30s | ${cs.label}%-10s | ${cs.avg_bytes}%12.2f | ${cs.avg_duration}%12.6f | ${cs.avg_packets}%12.2f | ${cs.count}%12d")
    }
  }

  // -------------------- UTILITY FUNCTIONS --------------------
  def saveResults(sc: SparkContext,
                  labelByCategory: org.apache.spark.rdd.RDD[TrafficClassStats],
                  ipProfileRDD: org.apache.spark.rdd.RDD[(String, IPProfile)]
                 ): Unit = {
    println("\n--- Writing results to files ---")
    val labelOutputPath = "output/label_by_category_rdd"
    val ipOutputPath = "output/ip_classifications_rdd"
    val fs = FileSystem.get(sc.hadoopConfiguration)

    // Delete output directories if they exist
    if (fs.exists(new Path(labelOutputPath))) fs.delete(new Path(labelOutputPath), true)
    if (fs.exists(new Path(ipOutputPath))) fs.delete(new Path(ipOutputPath), true)

    // Save RDDs
    labelByCategory
      .map { stats =>
        s"${stats.traffic_class},${stats.benign},${stats.malicious},${stats.total},${stats.benign_percent},${stats.malicious_percent}"}
      .coalesce(1)
      .saveAsTextFile(labelOutputPath)

    ipProfileRDD
      .map { case (_, profile) =>
        s"${profile.id_orig_h},${profile.avg_bytes_sent},${profile.total_bytes_sent},${profile.connection_count},${profile.avg_duration},${profile.traffic_class}"}
      .coalesce(1)
      .saveAsTextFile(ipOutputPath)
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