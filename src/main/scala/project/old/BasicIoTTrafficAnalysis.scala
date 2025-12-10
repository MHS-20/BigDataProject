package project.old

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import java.time.{Instant, ZoneId}

object BasicIoTTrafficAnalysis {

  // Case class for network traffic record
  case class NetworkTraffic(
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
                             orig_pkts: Long,
                             orig_ip_bytes: Long,
                             resp_pkts: Long,
                             resp_ip_bytes: Long,
                             tunnel_parents: String,
                             label: String
                           )

  // Helper function to parse a CSV line into NetworkTraffic
  def parseRecord(line: String): Option[NetworkTraffic] = {
    try {
      val fields = line.split(",", -1)
      if (fields.length == 22) {
        Some(NetworkTraffic(
          ts = fields(0).toDouble,
          uid = fields(1),
          id_orig_h = fields(2),
          id_orig_p = fields(3).toInt,
          id_resp_h = fields(4),
          id_resp_p = fields(5).toInt,
          proto = fields(6),
          service = if (fields(7) == "-") "" else fields(7),
          duration = if (fields(8).isEmpty) 0.0 else fields(8).toDouble,
          orig_bytes = if (fields(9).isEmpty) 0L else fields(9).toLong,
          resp_bytes = if (fields(10).isEmpty) 0L else fields(10).toLong,
          conn_state = fields(11),
          local_orig = if (fields(12) == "-") "" else fields(12),
          local_resp = if (fields(13) == "-") "" else fields(13),
          missed_bytes = if (fields(14).isEmpty) 0L else fields(14).toLong,
          history = fields(15),
          orig_pkts = if (fields(16).isEmpty) 0L else fields(16).toLong,
          orig_ip_bytes = if (fields(17).isEmpty) 0L else fields(17).toLong,
          resp_pkts = if (fields(18).isEmpty) 0L else fields(18).toLong,
          resp_ip_bytes = if (fields(19).isEmpty) 0L else fields(19).toLong,
          tunnel_parents = if (fields(20) == "-") "" else fields(20),
          label = fields(21).trim.toUpperCase
        ))
      } else {
        None
      }
    } catch {
      case _: Exception => None
    }
  }

  // Helper function to get hour from timestamp
  def getHour(timestamp: Double): Int = {
    val instant = Instant.ofEpochSecond(timestamp.toLong)
    val zdt = instant.atZone(ZoneId.systemDefault())
    zdt.getHour
  }

  // Helper function to format percentages
  def formatPercentage(count: Long, total: Long): String = {
    f"${(count * 100.0 / total)}%.2f"
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("IoT Network Traffic Analysis - RDD")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val dataPath = if (args.length > 0) args(0)
    else "C:\\Users\\muham\\Desktop\\Coding\\Unibo\\BigData\\BigDataProject\\datasets\\dataset1.csv"

    println("=" * 80)
    println("IoT NETWORK TRAFFIC ANALYSIS")
    println("=" * 80)

    // ========================================
    // 1. LOAD AND PREPARE DATA
    // ========================================

    println("\n[1] Loading dataset...")

    // Load and parse data
    val rawRDD = sc.textFile(dataPath)
    val header = rawRDD.first()

    val trafficRDD: RDD[NetworkTraffic] = rawRDD
      .filter(line => line != header)
      .flatMap(parseRecord)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val totalRecords = trafficRDD.count()
    println(s"Total records: $totalRecords")

    // Show sample
    println("\n--- Sample Records ---")
    trafficRDD.take(3).foreach { record =>
      println(s"${record.id_orig_h}:${record.id_orig_p} -> ${record.id_resp_h}:${record.id_resp_p} | ${record.proto} | ${record.label}")
    }

    // ========================================
    // 2. BASIC STATISTICS
    // ========================================

    println("\n[2] Computing basic statistics...")

    // Traffic distribution by protocol
    println("\n--- Traffic Distribution by Protocol ---")
    val protocolStats = trafficRDD
      .map(r => (r.proto, 1))
      .reduceByKey(_ + _)
      .map { case (proto, count) => (proto, count, formatPercentage(count, totalRecords)) }
      .sortBy(-_._2)
      .collect()

    protocolStats.foreach { case (proto, count, percentage) =>
      println(f"$proto%-10s | Count: $count%-10d | Percentage: $percentage%6s%%")
    }

    // Traffic distribution by label
    println("\n--- Traffic Distribution by Label ---")
    val labelStats = trafficRDD
      .map(r => (r.label, 1))
      .reduceByKey(_ + _)
      .map { case (label, count) => (label, count, formatPercentage(count, totalRecords)) }
      .sortBy(-_._2)
      .collect()

    labelStats.foreach { case (label, count, percentage) =>
      println(f"$label%-15s | Count: $count%-10d | Percentage: $percentage%6s%%")
    }

    // Connection state distribution
    println("\n--- Top 10 Connection States ---")
    val connStateStats = trafficRDD
      .map(r => (r.conn_state, 1))
      .reduceByKey(_ + _)
      .sortBy(-_._2)
      .take(10)

    connStateStats.foreach { case (state, count) =>
      println(f"$state%-10s | Count: $count%-10d")
    }

    // ========================================
    // 3. ATTACK DETECTION ANALYSIS
    // ========================================

    println("\n[3] Analyzing attack patterns...")

    val maliciousRDD = trafficRDD.filter(_.label != "BENIGN")
    val maliciousCount = maliciousRDD.count()

    println("\n--- Malicious Traffic Analysis ---")
    if (maliciousCount > 0) {
      val attackStats = maliciousRDD
        .map(r => (r.label, (1, r.duration, r.orig_bytes, r.resp_bytes, r.orig_pkts, r.resp_pkts)))
        .reduceByKey { case ((c1, d1, ob1, rb1, op1, rp1), (c2, d2, ob2, rb2, op2, rp2)) =>
          (c1 + c2, d1 + d2, ob1 + ob2, rb1 + rb2, op1 + op2, rp1 + rp2)
        }
        .map { case (label, (count, durSum, obSum, rbSum, opSum, rpSum)) =>
          (label, count, durSum / count, obSum / count, rbSum / count, opSum / count, rpSum / count)
        }
        .sortBy(-_._2)
        .collect()

      attackStats.foreach { case (label, count, avgDur, avgOB, avgRB, avgOP, avgRP) =>
        println(f"$label%-20s | Count: $count%-8d | Avg Duration: $avgDur%.4f | Avg Orig Bytes: $avgOB%.2f | Avg Resp Bytes: $avgRB%.2f")
      }
    } else {
      println("⚠️  WARNING: No malicious traffic found in dataset (all records are BENIGN)")
    }

    // ========================================
    // 4. SOURCE IP ANALYSIS
    // ========================================

    println("\n[4] Analyzing source IP addresses...")

    // Top source IPs by connection count
    println("\n--- Top 10 Source IPs by Connection Count ---")
    val sourceIPStats = trafficRDD
      .map(r => (r.id_orig_h, (1, r.orig_bytes, r.resp_bytes, Set(r.id_resp_h))))
      .reduceByKey { case ((c1, ob1, rb1, dests1), (c2, ob2, rb2, dests2)) =>
        (c1 + c2, ob1 + ob2, rb1 + rb2, dests1 ++ dests2)
      }
      .map { case (ip, (count, obSum, rbSum, dests)) =>
        (ip, count, obSum, rbSum, dests.size)
      }
      .sortBy(-_._2)
      .take(10)

    sourceIPStats.foreach { case (ip, count, obSum, rbSum, uniqueDests) =>
      println(f"$ip%-20s | Connections: $count%-8d | Orig Bytes: $obSum%-15d | Resp Bytes: $rbSum%-15d | Unique Dests: $uniqueDests%-5d")
    }

    // Most malicious source IPs
    if (maliciousCount > 0) {
      println("\n--- Top 10 Malicious Source IPs ---")
      val maliciousIPs = maliciousRDD
        .map(r => ((r.id_orig_h, r.label), 1))
        .reduceByKey(_ + _)
        .map { case ((ip, label), count) => (ip, label, count) }
        .sortBy(-_._3)
        .take(10)

      maliciousIPs.foreach { case (ip, label, count) =>
        println(f"$ip%-20s | Label: $label%-20s | Malicious Connections: $count%-8d")
      }
    }

    // ========================================
    // 5. DESTINATION PORT ANALYSIS
    // ========================================

    println("\n[5] Analyzing destination ports...")

    println("\n--- Top 20 Most Targeted Ports ---")
    val portStats = trafficRDD
      .map(r => (r.id_resp_p, (1, if (r.label != "BENIGN") 1 else 0)))
      .reduceByKey { case ((c1, m1), (c2, m2)) => (c1 + c2, m1 + m2) }
      .map { case (port, (total, malicious)) =>
        (port, total, malicious, formatPercentage(malicious, total))
      }
      .sortBy(-_._2)
      .take(20)

    portStats.foreach { case (port, total, malicious, percentage) =>
      println(f"Port: $port%-8d | Connections: $total%-10d | Malicious: $malicious%-8d | Malicious %%: $percentage%6s%%")
    }

    // ========================================
    // 6. TIME-BASED ANALYSIS
    // ========================================

    println("\n[6] Performing time-based analysis...")

    println("\n--- Traffic Distribution by Hour ---")
    val hourlyStats = trafficRDD
      .map(r => (getHour(r.ts), (1, if (r.label != "BENIGN") 1 else 0)))
      .reduceByKey { case ((t1, m1), (t2, m2)) => (t1 + t2, m1 + m2) }
      .sortBy(_._1)
      .collect()

    hourlyStats.foreach { case (hour, (total, malicious)) =>
      println(f"Hour: $hour%02d | Total: $total%-10d | Malicious: $malicious%-8d")
    }

    // ========================================
    // 7. BANDWIDTH ANALYSIS
    // ========================================

    println("\n[7] Analyzing bandwidth usage...")

    println("\n--- Bandwidth Usage by Traffic Type ---")
    val bandwidthStats = trafficRDD
      .map(r => (r.label, (r.orig_bytes, r.resp_bytes, 1)))
      .reduceByKey { case ((ob1, rb1, c1), (ob2, rb2, c2)) =>
        (ob1 + ob2, rb1 + rb2, c1 + c2)
      }
      .map { case (label, (obSum, rbSum, count)) =>
        (label, obSum / 1024.0 / 1024.0, rbSum / 1024.0 / 1024.0,
          (obSum + rbSum) / 1024.0 / 1024.0, obSum.toDouble / count, rbSum.toDouble / count)
      }
      .collect()

    bandwidthStats.foreach { case (label, origMB, respMB, totalMB, avgOrig, avgResp) =>
      println(f"$label%-15s | Orig MB: $origMB%10.2f | Resp MB: $respMB%10.2f | Total MB: $totalMB%10.2f | Avg Orig: $avgOrig%10.2f | Avg Resp: $avgResp%10.2f")
    }

    // High bandwidth connections
    println("\n--- Top 10 High Bandwidth Connections ---")
    val highBandwidth = trafficRDD
      .map(r => (r.id_orig_h, r.id_resp_h, r.id_resp_p, r.proto, r.orig_bytes + r.resp_bytes, r.label))
      .sortBy(-_._5)
      .take(10)

    highBandwidth.foreach { case (origIP, respIP, port, proto, totalBytes, label) =>
      println(f"$origIP%-20s -> $respIP%-20s:$port%-8d | $proto%-6s | Bytes: $totalBytes%-15d | $label")
    }

    // ========================================
    // 8. ANOMALY DETECTION - STATISTICAL APPROACH
    // ========================================

    println("\n[8] Detecting statistical anomalies...")

    // Calculate duration statistics
    val durationData = trafficRDD
      .filter(_.duration > 0)
      .map(_.duration)
      .persist()

    val durationCount = durationData.count()
    val durationSum = durationData.reduce(_ + _)
    val meanDuration = durationSum / durationCount
    val variance = durationData.map(d => math.pow(d - meanDuration, 2)).reduce(_ + _) / durationCount
    val stddevDuration = math.sqrt(variance)

    println(s"\n--- Duration Statistics ---")
    println(f"Mean: $meanDuration%.4f | Std Dev: $stddevDuration%.4f")

    println("\n--- Abnormal Duration Connections (> mean + 3*stddev) ---")
    val threshold = meanDuration + 3 * stddevDuration
    val abnormalDuration = trafficRDD
      .filter(_.duration > threshold)
      .map(r => (r.id_orig_h, r.id_resp_h, r.id_resp_p, r.duration, r.orig_bytes, r.resp_bytes, r.label))
      .sortBy(-_._4)
      .take(10)

    abnormalDuration.foreach { case (origIP, respIP, port, dur, ob, rb, label) =>
      println(f"$origIP%-20s -> $respIP%-20s:$port%-8d | Duration: $dur%10.4f | Orig: $ob%-12d | Resp: $rb%-12d | $label")
    }

    durationData.unpersist()

    // Calculate byte statistics
    val bytesData = trafficRDD
      .filter(_.orig_bytes > 0)
      .map(_.orig_bytes.toDouble)
      .persist()

    val bytesCount = bytesData.count()
    val bytesSum = bytesData.reduce(_ + _)
    val meanBytes = bytesSum / bytesCount
    val bytesVariance = bytesData.map(b => math.pow(b - meanBytes, 2)).reduce(_ + _) / bytesCount
    val stddevBytes = math.sqrt(bytesVariance)

    println(s"\n--- Byte Transfer Statistics ---")
    println(f"Mean: $meanBytes%.2f | Std Dev: $stddevBytes%.2f")

    println("\n--- Abnormal Byte Transfer Connections (> mean + 3*stddev) ---")
    val bytesThreshold = meanBytes + 3 * stddevBytes
    val abnormalBytes = trafficRDD
      .filter(_.orig_bytes > bytesThreshold.toLong)
      .map(r => (r.id_orig_h, r.id_resp_h, r.id_resp_p, r.orig_bytes, r.resp_bytes, r.label))
      .sortBy(-_._4)
      .take(10)

    abnormalBytes.foreach { case (origIP, respIP, port, ob, rb, label) =>
      println(f"$origIP%-20s -> $respIP%-20s:$port%-8d | Orig Bytes: $ob%-15d | Resp Bytes: $rb%-15d | $label")
    }

    bytesData.unpersist()

    // ========================================
    // 9. PROTOCOL-SPECIFIC ANALYSIS
    // ========================================

    println("\n[9] Protocol-specific analysis...")

    // UDP traffic
    println("\n--- UDP Traffic Analysis ---")
    val udpStats = trafficRDD
      .filter(_.proto == "udp")
      .map(r => (r.label, (1, r.orig_bytes, r.resp_bytes, Set(r.id_resp_p))))
      .reduceByKey { case ((c1, ob1, rb1, ports1), (c2, ob2, rb2, ports2)) =>
        (c1 + c2, ob1 + ob2, rb1 + rb2, ports1 ++ ports2)
      }
      .map { case (label, (count, obSum, rbSum, ports)) =>
        (label, count, obSum.toDouble / count, rbSum.toDouble / count, ports.size)
      }
      .collect()

    udpStats.foreach { case (label, count, avgOB, avgRB, uniquePorts) =>
      println(f"$label%-15s | Count: $count%-10d | Avg Orig: $avgOB%12.2f | Avg Resp: $avgRB%12.2f | Unique Ports: $uniquePorts%-6d")
    }

    // TCP traffic
    println("\n--- TCP Traffic Analysis ---")
    val tcpStats = trafficRDD
      .filter(_.proto == "tcp")
      .map(r => ((r.label, r.conn_state), 1))
      .reduceByKey(_ + _)
      .map { case ((label, state), count) => (label, state, count) }
      .sortBy(-_._3)
      .take(20)

    tcpStats.foreach { case (label, state, count) =>
      println(f"$label%-15s | State: $state%-10s | Count: $count%-10d")
    }

    // ICMP traffic
    println("\n--- ICMP Traffic Analysis ---")
    val icmpCount = trafficRDD.filter(_.proto == "icmp").count()
    if (icmpCount > 0) {
      val icmpStats = trafficRDD
        .filter(_.proto == "icmp")
        .map(r => (r.label, (1, r.orig_bytes)))
        .reduceByKey { case ((c1, ob1), (c2, ob2)) => (c1 + c2, ob1 + ob2) }
        .map { case (label, (count, obSum)) => (label, count, obSum.toDouble / count) }
        .collect()

      icmpStats.foreach { case (label, count, avgOB) =>
        println(f"$label%-15s | Count: $count%-10d | Avg Orig Bytes: $avgOB%12.2f")
      }
    } else {
      println("No ICMP traffic found")
    }

    // ========================================
    // 10. SERVICE ANALYSIS
    // ========================================

    println("\n[10] Analyzing services...")

    println("\n--- Top Services by Connection Count ---")
    val serviceStats = trafficRDD
      .filter(_.service.nonEmpty)
      .map(r => (r.service, (1, if (r.label != "BENIGN") 1 else 0)))
      .reduceByKey { case ((c1, m1), (c2, m2)) => (c1 + c2, m1 + m2) }
      .map { case (service, (total, malicious)) => (service, total, malicious) }
      .sortBy(-_._2)
      .take(15)

    serviceStats.foreach { case (service, total, malicious) =>
      println(f"$service%-20s | Connections: $total%-10d | Malicious: $malicious%-8d")
    }

    // ========================================
    // 11. SUMMARY REPORT
    // ========================================

    println("\n[11] Generating summary report...")
    println("\n" + "=" * 80)
    println("SUMMARY REPORT")
    println("=" * 80)

    val benignCount = trafficRDD.filter(_.label == "BENIGN").count()
    val uniqueSources = trafficRDD.map(_.id_orig_h).distinct().count()
    val uniqueDestinations = trafficRDD.map(_.id_resp_h).distinct().count()
    val uniquePorts = trafficRDD.map(_.id_resp_p).distinct().count()

    println(s"Total Records: $totalRecords")
    println(s"Benign Traffic: $benignCount (${formatPercentage(benignCount, totalRecords)}%)")
    println(s"Malicious Traffic: $maliciousCount (${formatPercentage(maliciousCount, totalRecords)}%)")
    println(s"Unique Source IPs: $uniqueSources")
    println(s"Unique Destination IPs: $uniqueDestinations")
    println(s"Unique Destination Ports: $uniquePorts")
    println("\nProtocol Distribution:")
    protocolStats.foreach { case (proto, count, _) =>
      println(s"  - ${proto.toUpperCase}: $count")
    }
    println("=" * 80)

    // Unpersist RDD
    trafficRDD.unpersist()

    sc.stop()
  }
}