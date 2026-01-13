package project.optv1

import org.apache.spark.{SparkConf, SparkContext}
import project.IoTTrafficUtilities._
import project.{CategoryStats, EnrichedRecord, IPProfile}

object IoTTrafficAnalysisNaive {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("IoT Traffic Analysis - Naive")
      .setMaster("local[*]")
      .set("spark.driver.memory", "6g")
      .set("spark.executor.memory", "6g")
      .set("spark.driver.extraJavaOptions", "-Xmx6g -Xms4g")
    // Anti-pattern: Using default Java serialization (slow)
    // Anti-pattern: Not tuning shuffle partitions

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    println("=== IoT Traffic Analysis (NAIVE - NOT OPTIMIZED) ===")

    // Anti-pattern: Reading file multiple times instead of caching early
    val rawData = sc.textFile("C:\\Users\\muham\\Desktop\\Coding\\Unibo\\BigData\\BigDataProject\\datasets\\dataset52.csv")

    val header = rawData.first()

    // Anti-pattern: Multiple passes over data, no caching
    val dataRDD = rawData
      .filter(line => line != header)
      .map(parseLine)
      .filter(_.isDefined)
      .map(_.get)
    // NOT CACHED - will be recomputed every time it's used!

    println(s"\nTotal records loaded: ${dataRDD.count()}") // First computation

    // SHUFFLE 1: Basic IP aggregation (recomputes dataRDD)
    val ipBytes = dataRDD
      .map(record => (record.id_orig_h, record.orig_bytes))
      .reduceByKey(_ + _)

    // SHUFFLE 2: IP connection counts (recomputes dataRDD again)
    val ipCounts = dataRDD
      .map(record => (record.id_orig_h, 1L))
      .reduceByKey(_ + _)

    // SHUFFLE 3: IP durations (recomputes dataRDD yet again)
    val ipDurations = dataRDD
      .map(record => (record.id_orig_h, record.duration))
      .reduceByKey(_ + _)

    // SHUFFLE 4-5: Joining all IP metrics separately (very expensive)
    val ipBytesAndCounts = ipBytes.join(ipCounts) // Join 1
    val ipAllMetrics = ipBytesAndCounts.join(ipDurations) // Join 2

    // Anti-pattern: Multiple operations after expensive joins
    val ipProfileRDD = ipAllMetrics
      .map { case (ip, ((totalBytes, count), totalDur)) =>
        val avgBytes = totalBytes.toDouble / count
        val avgDur = totalDur / count
        val trafficClass = classifyTraffic(count, avgBytes)
        (ip, IPProfile(ip, avgBytes, totalBytes, count, avgDur, trafficClass))
      }
    // NOT CACHED

    printIPProfiles(ipProfileRDD.take(20))

    // SHUFFLE 6: Expensive join with original dataset (recomputes dataRDD)
    val trafficWithIP = dataRDD.map(r => (r.id_orig_h, r))
    val enrichedRDD = trafficWithIP
      .join(ipProfileRDD) // Very expensive shuffle join
      .map { case (ip, (record, profile)) => EnrichedRecord(record, profile) }
    // NOT CACHED

    println(enrichedRDD.take(5).mkString("Array(", ", ", ")"))

    // SHUFFLE 7: Computing label distribution from scratch
    val labelCounts = enrichedRDD
      .map(e => (e.record.label, 1L))
      .reduceByKey(_ + _)
      .collect() // Anti-pattern: Collecting large datasets

    // SHUFFLE 8: Traffic class counts
    val trafficClassCounts = enrichedRDD
      .map(e => (e.profile.traffic_class, 1L))
      .reduceByKey(_ + _)

    // SHUFFLE 9-10: Cross aggregation (very expensive)
    val labelByCategory = enrichedRDD
      .map(e => ((e.profile.traffic_class, e.record.label), 1L))
      .reduceByKey(_ + _)
      .map { case ((tc, lbl), cnt) => (tc, (lbl, cnt)) }
      .groupByKey() // Anti-pattern: groupByKey instead of aggregateByKey
      .map { case (tc, labelCounts) =>
        val countsMap = labelCounts.toMap
        createTrafficClassStats(tc, countsMap)
      }
      .sortBy(_.traffic_class)

    printLabelDistribution(labelByCategory.take(20))

    // SHUFFLE 11-14: Recomputing statistics separately for each metric
    val bytesStats = enrichedRDD
      .map(e => ((e.profile.traffic_class, e.record.label), e.record.orig_bytes))
      .groupByKey() // Anti-pattern: groupByKey with large value lists
      .map { case (key, bytes) => (key, bytes.sum / bytes.size.toDouble) }

    val durationStats = enrichedRDD
      .map(e => ((e.profile.traffic_class, e.record.label), e.record.duration))
      .groupByKey() // Another expensive groupByKey
      .map { case (key, durs) => (key, durs.sum / durs.size.toDouble) }

    val packetStats = enrichedRDD
      .map(e => ((e.profile.traffic_class, e.record.label), e.record.orig_pkts))
      .groupByKey() // Yet another expensive groupByKey
      .map { case (key, pkts) => (key, pkts.sum / pkts.size.toDouble) }

    val countStats = enrichedRDD
      .map(e => ((e.profile.traffic_class, e.record.label), 1L))
      .reduceByKey(_ + _)

    // SHUFFLE 15-17: Multiple joins to combine stats
    val stats1 = bytesStats.join(durationStats)
    val stats2 = stats1.join(packetStats)
    val categoryStats = stats2.join(countStats)
      .map { case ((tc, lbl), (((avgB, avgD), avgP), cnt)) =>
        CategoryStats(tc, lbl, avgB, avgD, avgP, cnt)
      }
      .sortBy(cs => (cs.traffic_class, cs.label))

    printCategoryStats(categoryStats.take(20))

    // Anti-pattern: Collecting potentially large RDDs
    saveResults(sc, labelByCategory, ipProfileRDD)

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
}

/*
ANTI-PATTERNS IN NAIVE VERSION:
================================
1. **No Caching**: dataRDD recomputed 6+ times (massive waste)
2. **Excessive Shuffles**: 15-17 shuffles vs 1-2 in optimized version
3. **Multiple Passes**: Separate aggregations for each metric
4. **Expensive Joins**: Multiple joins instead of single aggregation
5. **groupByKey**: Uses groupByKey which loads all values in memory
6. **No Broadcast**: Large join instead of broadcast for small datasets
7. **Default Serialization**: Slow Java serialization
8. **Collect() Misuse**: Collecting large intermediate results
9. **No Storage Level Tuning**: Uses default caching strategy
10. **No Cleanup**: Never unpersists cached data

PERFORMANCE IMPACT:
===================
- 3-5x slower execution time
- 2-3x higher memory usage
- 10-15x more network shuffle traffic
- Risk of OOM errors with large datasets
- Poor cluster resource utilization

This version demonstrates common mistakes that lead to poor Spark performance.
*/