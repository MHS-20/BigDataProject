package project

import breeze.plot._
import java.io.File

object IoTTrafficVisualization {

  private def ensureDir(path: String): Unit = {
    val dir = new File(path)
    if (!dir.exists()) dir.mkdirs()
  }

  def generateAllCharts(
                         categoryStats: org.apache.spark.rdd.RDD[CategoryStats],
                         ipProfiles: org.apache.spark.rdd.RDD[(String, IPProfile)],
                         enriched: org.apache.spark.rdd.RDD[EnrichedRecord],
                         outputDir: String
                       ): Unit = {

    ensureDir(outputDir)

    saveCategoryStats(categoryStats.collect(), s"$outputDir/category_stats.png")
    saveBytesDistribution(ipProfiles.collect(), s"$outputDir/bytes_distribution.png")
    saveAvgBytes(ipProfiles.collect(), s"$outputDir/avg_bytes.png")
    saveTrafficClassDistribution(ipProfiles.collect(), s"$outputDir/traffic_class_distribution.png")
    saveBenignMalicious(categoryStats.collect(), s"$outputDir/benign_vs_malicious.png")
  }


  /** =====================================
   * 1. Traffic Class - Bar Chart
   * ===================================== */
  private def saveCategoryStats(stats: Seq[CategoryStats], out: String): Unit = {
    val grouped = stats.groupBy(_.traffic_class).mapValues(_.map(_.count).sum).toSeq

    val f = Figure()
    val p = f.subplot(0)

    p += breeze.plot.plot(
      x = breeze.linalg.DenseVector((0 until grouped.size).map(_.toDouble).toArray),
      y = breeze.linalg.DenseVector(grouped.map(_._2.toDouble).toArray),
      name = "Counts"
    )

    p.xlabel = "Traffic Class"
    p.ylabel = "Count"
    p.title = "Traffic Class Counts"

    // Add categorical labels
//    p.xticks((0 until grouped.size).map(_.toDouble).toArray,
//      grouped.map(_._1).toArray)

    f.saveas(out)
  }


  /** =====================================
   * 2. Bytes Distribution - Histogram
   * ===================================== */
  private def saveBytesDistribution(profiles: Array[(String, IPProfile)], out: String): Unit = {

    val values = profiles.map(_._2.total_bytes_sent.toDouble)

    val f = Figure()
    val p = f.subplot(0)

    p += hist(breeze.linalg.DenseVector(values), 50)

    p.xlabel = "Total Bytes Sent"
    p.ylabel = "Frequency"
    p.title = "Distribution of Total Bytes Sent"

    f.saveas(out)
  }


  /** =====================================
   * 3. Avg Bytes per Traffic Class
   * ===================================== */
  private def saveAvgBytes(profiles: Array[(String, IPProfile)], out: String): Unit = {

    val grouped = profiles.groupBy(_._2.traffic_class)
      .map { case (tc, arr) =>
        tc -> arr.map(_._2.avg_bytes_sent).sum / arr.length
      }.toSeq

    val f = Figure()
    val p = f.subplot(0)

    p += breeze.plot.plot(
      x = breeze.linalg.DenseVector((0 until grouped.size).map(_.toDouble).toArray),
      y = breeze.linalg.DenseVector(grouped.map(_._2).toArray)
    )

    p.xlabel = "Traffic Class"
    p.ylabel = "Avg Bytes"
    p.title = "Average Bytes per Traffic Class"

//    p.xticks((0 until grouped.size).map(_.toDouble).toArray,
//      grouped.map(_._1).toArray)

    f.saveas(out)
  }


  /** =====================================
   * 4. Traffic Class Distribution - Bar Chart
   * ===================================== */
  private def saveTrafficClassDistribution(profiles: Array[(String, IPProfile)], out: String): Unit = {

    val grouped = profiles.groupBy(_._2.traffic_class)
      .mapValues(_.length.toDouble).toSeq

    val f = Figure()
    val p = f.subplot(0)

    p += breeze.plot.plot(
      x = breeze.linalg.DenseVector((0 until grouped.size).map(_.toDouble).toArray),
      y = breeze.linalg.DenseVector(grouped.map(_._2).toArray)
    )

    p.xlabel = "Traffic Class"
    p.ylabel = "Count"
    p.title = "Traffic Class Distribution"

    // p.xticks((0 until grouped.size).map(_.toDouble).toArray,
     //  grouped.map(_._1).toArray)

    f.saveas(out)
  }


  /** =====================================
   * 5. Benign vs Malicious - Bar Chart
   * ===================================== */
  private def saveBenignMalicious(stats: Seq[CategoryStats], out: String): Unit = {
    val benign = stats.filter(_.label == "Benign").map(_.count).sum
    val mal = stats.filter(_.label != "Benign").map(_.count).sum

    val categories = Seq("Benign", "Malicious")
    val values = Seq(benign.toDouble, mal.toDouble)

    val f = Figure()
    val p = f.subplot(0)

    p += breeze.plot.plot(
      x = breeze.linalg.DenseVector(0.0, 1.0),
      y = breeze.linalg.DenseVector(values.toArray)
    )

    p.xlabel = "Label"
    p.ylabel = "Count"
    p.title = "Benign vs Malicious Traffic"

    // p.xticks(Array(0.0, 1.0), categories.toArray)

    f.saveas(out)
  }
}
