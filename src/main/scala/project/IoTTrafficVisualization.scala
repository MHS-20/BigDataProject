package project

import breeze.plot._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import utils.Commons.getChartPath

import java.io.{ByteArrayOutputStream, File}

object IoTTrafficVisualization {

  /** ============================================================
   *   Utility: Save Breeze figure to Hadoop FS (local or S3)
   * ============================================================ */
  private def saveFigure(f: Figure, dst: String, fs: FileSystem): Unit = {
    val baos = new ByteArrayOutputStream()

    // Breeze saves only to file path, so we save into temp file first
    val tmp = File.createTempFile("chart_", ".png")
    f.saveas(tmp.getAbsolutePath)

    // Now write to Hadoop filesystem
    val outPath = new Path(dst)
    val outStream = fs.create(outPath, true)

    val fileBytes = java.nio.file.Files.readAllBytes(tmp.toPath)
    outStream.write(fileBytes)
    outStream.close()

    tmp.delete()
  }

  /** ============================================================
   *   Generate all charts (with S3-safe saving)
   * ============================================================ */
  def generateAllCharts(
                         mode: String,
                         sc: SparkContext,
                         spark: SparkSession,
                         categoryStats: org.apache.spark.rdd.RDD[CategoryStats],
                         ipProfiles: org.apache.spark.rdd.RDD[(String, IPProfile)],
                         enriched: org.apache.spark.rdd.RDD[EnrichedRecord]
                       ): Unit = {

    import spark.implicits._
    val outputDir = getChartPath(mode)
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val chartPath = new Path(s"$outputDir/")
    if (fs.exists(chartPath)) fs.delete(chartPath, true)
    fs.mkdirs(chartPath)

    saveCategoryStats(categoryStats.collect(), s"$outputDir/category_stats.png", fs)
    saveBytesDistribution(ipProfiles.collect(), s"$outputDir/bytes_distribution.png", fs)
    saveAvgBytes(ipProfiles.collect(), s"$outputDir/avg_bytes.png", fs)
    saveTrafficClassDistribution(ipProfiles.collect(), s"$outputDir/traffic_class_distribution.png", fs)
    saveBenignMalicious(categoryStats.collect(), s"$outputDir/benign_vs_malicious.png", fs)
  }

  /** ============================================================
   * 1. Traffic Class Stats
   * ============================================================ */
  private def saveCategoryStats(stats: Seq[CategoryStats], out: String, fs: FileSystem): Unit = {
    val grouped = stats.groupBy(_.traffic_class).mapValues(_.map(_.count).sum).toSeq

    val f = Figure()
    val p = f.subplot(0)

    p += breeze.plot.plot(
      x = breeze.linalg.DenseVector((0 until grouped.size).map(_.toDouble).toArray),
      y = breeze.linalg.DenseVector(grouped.map(_._2.toDouble).toArray)
    )

    p.xlabel = "Traffic Class"
    p.ylabel = "Count"
    p.title = "Traffic Class Counts"

    saveFigure(f, out, fs)
  }

  /** ============================================================
   * 2. Bytes Distribution Histogram
   * ============================================================ */
  private def saveBytesDistribution(profiles: Array[(String, IPProfile)], out: String, fs: FileSystem): Unit = {
    val values = profiles.map(_._2.total_bytes_sent.toDouble)

    val f = Figure()
    val p = f.subplot(0)

    p += hist(breeze.linalg.DenseVector(values), 50)

    p.xlabel = "Total Bytes Sent"
    p.ylabel = "Frequency"
    p.title = "Distribution of Total Bytes Sent"

    saveFigure(f, out, fs)
  }

  /** ============================================================
   * 3. Average Bytes per Traffic Class
   * ============================================================ */
  private def saveAvgBytes(profiles: Array[(String, IPProfile)], out: String, fs: FileSystem): Unit = {
    val grouped = profiles
      .groupBy(_._2.traffic_class)
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

    saveFigure(f, out, fs)
  }

  /** ============================================================
   * 4. Traffic Class Distribution
   * ============================================================ */
  private def saveTrafficClassDistribution(profiles: Array[(String, IPProfile)], out: String, fs: FileSystem): Unit = {
    val grouped = profiles.groupBy(_._2.traffic_class).mapValues(_.length.toDouble).toSeq

    val f = Figure()
    val p = f.subplot(0)

    p += breeze.plot.plot(
      x = breeze.linalg.DenseVector((0 until grouped.size).map(_.toDouble).toArray),
      y = breeze.linalg.DenseVector(grouped.map(_._2).toArray)
    )

    p.xlabel = "Traffic Class"
    p.ylabel = "Count"
    p.title = "Traffic Class Distribution"

    saveFigure(f, out, fs)
  }

  /** ============================================================
   * 5. Benign vs Malicious
   * ============================================================ */
  private def saveBenignMalicious(stats: Seq[CategoryStats], out: String, fs: FileSystem): Unit = {
    val benign = stats.filter(_.label == "Benign").map(_.count).sum
    val mal = stats.filter(_.label != "Benign").map(_.count).sum

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

    saveFigure(f, out, fs)
  }
}