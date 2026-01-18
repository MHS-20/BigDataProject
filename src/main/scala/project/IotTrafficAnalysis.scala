package project

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import project.IoTTrafficUtilities._
import utils.Commons._

object IoTTrafficAnalysis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("IoT Traffic Analysis")
      .setMaster("local[*]")
      .set("spark.driver.memory", "10g")
      .set("spark.executor.memory", "10g")
      .set("spark.driver.extraJavaOptions", "-Xmx8g -Xms6g")

    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    initializeSparkContext("remote", sc)
    sc.setLogLevel("ERROR")

    println("=== IoT Traffic Analysis ===")
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

    ipProfileRDD.cache()
    printIPProfiles(ipProfileRDD.collect())

    // SECOND SHUFFLE: Join back with original dataset
    val trafficWithIP = dataRDD.map(r => (r.id_orig_h, r))
    val enrichedRDD = trafficWithIP
      .join(ipProfileRDD)
      .map { case (_, (record, profile)) =>
        EnrichedRecord(record, profile)
      }

    enrichedRDD.cache()

    // THIRD SHUFFLE: Statistical summary by category
    val categoryStats = enrichedRDD
      .map(e => ((e.profile.traffic_class, e.record.label), (e.record.orig_bytes, e.record.duration, e.record.orig_pkts, 1L)))
      .reduceByKey { case ((b1, d1, p1, c1), (b2, d2, p2, c2)) => (b1 + b2, d1 + d2, p1 + p2, c1 + c2) }
      .map { case ((tc, lbl), (totB, totD, totP, cnt)) =>
        CategoryStats(tc, lbl, totB.toDouble / cnt, totD / cnt, totP.toDouble / cnt, cnt) }

    printCategoryStats(categoryStats.collect())
    saveResults(sc, spark, args{0}, categoryStats, ipProfileRDD)
    println("\n=== Analysis Complete ===")

    // ============= VISUALIZZAZIONI  =============
    println("\n=== Generating Visualizations ===")

    try {
      IoTTrafficVisualization.generateAllCharts(args{0}, sc, spark, categoryStats, ipProfileRDD, enrichedRDD)
      println("All visualizations saved in 'charts' directory")
    } catch {
      case e: Exception =>
        println(s"Error generating charts: ${e.getMessage}")
        e.printStackTrace()
    }

    println("\n=== Analysis Complete ===")
    spark.stop()
    spark.close()
    sc.stop()
  }
}