package project

import breeze.plot._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import utils.Commons.getChartPath
import org.jfree.chart.axis.{SymbolAxis, NumberAxis}

import java.io.{ByteArrayOutputStream, File}

object IoTTrafficVisualization {

  private def saveFigure(f: Figure, dst: String, fs: FileSystem): Unit = {
    val tmp = File.createTempFile("chart_", ".png")
    f.saveas(tmp.getAbsolutePath)

    val outPath = new Path(dst)
    val outStream = fs.create(outPath, true)

    val fileBytes = java.nio.file.Files.readAllBytes(tmp.toPath)
    outStream.write(fileBytes)
    outStream.close()

    tmp.delete()
  }

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

    println("\n=== Generating Charts ===")

    val statsCollected = categoryStats.collect()
    val profilesCollected = ipProfiles.collect()
    val enrichedCollected = enriched.collect()

    // 1. Distribuzione traffico per classe (con percentuali malicious)
    saveTrafficClassAnalysis(statsCollected, s"$outputDir/1_traffic_by_class.png", fs)

    // 2. Pattern comportamentali: bytes vs duration
    saveBehavioralScatter(enrichedCollected, s"$outputDir/2_behavior_scatter.png", fs)

    // 3. Top IP per volume di traffico
    saveTopTalkers(profilesCollected, s"$outputDir/3_top_talkers.png", fs)

    println(s"=== Charts saved to: $outputDir ===\n")
  }

  /** ============================================================
   * 1. Analisi per Traffic Class con percentuale malicious
   * ============================================================ */
  private def saveTrafficClassAnalysis(stats: Seq[CategoryStats], out: String, fs: FileSystem): Unit = {
    // Calcola statistiche per classe
    val byClass = stats.groupBy(_.traffic_class).map { case (tc, items) =>
      val benignCount = items.filter(_.label == "benign").map(_.count).sum
      val maliciousCount = items.filter(_.label != "benign").map(_.count).sum
      val totalCount = benignCount + maliciousCount
      val maliciousPercent = if (totalCount > 0) (maliciousCount.toDouble / totalCount) * 100 else 0.0

      (tc, benignCount, maliciousCount, totalCount, maliciousPercent)
    }.toSeq.sortBy(-_._4) // Ordina per totale decrescente

    if (byClass.isEmpty) {
      println("  ! No data for traffic class analysis")
      return
    }

    val classes = byClass.map(_._1).toArray
    val maliciousPercents = byClass.map(_._5).toArray
    val totals = byClass.map(_._4).toArray

    val f = Figure()
    f.width = 800
    f.height = 500

    // Subplot 1: Percentuale malicious per classe
    val p1 = f.subplot(2, 1, 0)
    val indices = (0 until classes.length).map(_.toDouble).toArray

    p1 += plot(
      breeze.linalg.DenseVector(indices),
      breeze.linalg.DenseVector(maliciousPercents),
      style = '+'
    )

    val xaxis1 = new SymbolAxis("", classes)
    p1.plot.setDomainAxis(xaxis1)
    p1.ylabel = "Malicious %"
    p1.title = "Security Risk by Traffic Class"

    // Subplot 2: Volume totale connessioni
    val p2 = f.subplot(2, 1, 1)
    p2 += plot(
      breeze.linalg.DenseVector(indices),
      breeze.linalg.DenseVector(totals.map(_.toDouble)),
      style = '-'
    )

    val xaxis2 = new SymbolAxis("Traffic Class", classes)
    xaxis2.setVerticalTickLabels(true)
    p2.plot.setDomainAxis(xaxis2)
    p2.ylabel = "Total Connections"

    println(s"  → traffic_by_class.png")
    byClass.foreach { case (tc, b, m, t, mp) =>
      println(f"     $tc%-30s: $t%7d connections ($mp%5.1f%% malicious)")
    }

    saveFigure(f, out, fs)
  }

  /** ============================================================
   * 2. Scatter Plot Comportamentale: Bytes vs Duration
   * ============================================================ */
  private def saveBehavioralScatter(enriched: Array[EnrichedRecord], out: String, fs: FileSystem): Unit = {
    // Filtra record validi e rimuovi outlier estremi
    val filtered = enriched.filter(e =>
      e.record.duration > 0 &&
        e.record.duration < 1000 && // Max 1000 secondi
        e.record.orig_bytes > 0 &&
        e.record.orig_bytes < 1e8 // Max 100MB
    )

    val benign = filtered.filter(_.record.label == "benign")
    val malicious = filtered.filter(_.record.label != "benign")

    // Campionamento per performance
    val maxSample = 2000
    val benignSample = if (benign.length > maxSample)
      scala.util.Random.shuffle(benign.toSeq).take(maxSample).toArray
    else benign

    val maliciousSample = if (malicious.length > maxSample)
      scala.util.Random.shuffle(malicious.toSeq).take(maxSample).toArray
    else malicious

    if (benignSample.isEmpty && maliciousSample.isEmpty) {
      println("  ! No valid data for behavioral scatter")
      return
    }

    val f = Figure()
    f.width = 800
    f.height = 600
    val p = f.subplot(0)

    // Plot benign (blu)
    if (benignSample.nonEmpty) {
      val x = benignSample.map(e => math.log10(e.record.duration + 0.001))
      val y = benignSample.map(e => math.log10(e.record.orig_bytes.toDouble + 1))
      p += plot(
        breeze.linalg.DenseVector(x),
        breeze.linalg.DenseVector(y),
        '.',
        colorcode = "blue"
      )
    }

    // Plot malicious (rosso) - sovrapposto
    if (maliciousSample.nonEmpty) {
      val x = maliciousSample.map(e => math.log10(e.record.duration + 0.001))
      val y = maliciousSample.map(e => math.log10(e.record.orig_bytes.toDouble + 1))
      p += plot(
        breeze.linalg.DenseVector(x),
        breeze.linalg.DenseVector(y),
        '.',
        colorcode = "red"
      )
    }

    p.xlabel = "Log10(Duration [seconds])"
    p.ylabel = "Log10(Bytes Sent)"
    p.title = "Behavioral Patterns: Blue=Benign, Red=Malicious"

    println(s"  → behavior_scatter.png")
    println(f"     Benign: ${benignSample.length}%5d samples, Malicious: ${maliciousSample.length}%5d samples")

    if (benign.nonEmpty) {
      val avgDur = benign.map(_.record.duration).sum / benign.length
      val avgBytes = benign.map(_.record.orig_bytes).sum / benign.length
      println(f"     Benign avg: ${avgDur}%.2f s, ${avgBytes/1024}%.0f KB")
    }

    if (malicious.nonEmpty) {
      val avgDur = malicious.map(_.record.duration).sum / malicious.length
      val avgBytes = malicious.map(_.record.orig_bytes).sum / malicious.length
      println(f"     Malicious avg: ${avgDur}%.2f s, ${avgBytes/1024}%.0f KB")
    }

    saveFigure(f, out, fs)
  }

  /** ============================================================
   * 3. Top Talkers - IP con più traffico
   * ============================================================ */
  private def saveTopTalkers(profiles: Array[(String, IPProfile)], out: String, fs: FileSystem): Unit = {
    val topN = 20
    val top = profiles
      .filter(_._2.total_bytes_sent > 0)
      .sortBy(-_._2.total_bytes_sent)
      .take(topN)

    if (top.isEmpty) {
      println("  ! No data for top talkers")
      return
    }

    // Prepara dati
    val labels = top.zipWithIndex.map { case ((ip, profile), idx) =>
      val shortIp = if (ip.length > 15) ip.take(12) + "..." else ip
      val cls = profile.traffic_class
      s"$shortIp [$cls]"
    }

    val bytesMB = top.map(_._2.total_bytes_sent.toDouble / (1024 * 1024))
    val connections = top.map(_._2.connection_count.toDouble)

    val f = Figure()
    f.width = 1000
    f.height = 600

    // Subplot 1: Bytes inviati
    val p1 = f.subplot(2, 1, 0)
    val indices = (0 until top.length).map(_.toDouble).toArray

    p1 += plot(
      breeze.linalg.DenseVector(indices),
      breeze.linalg.DenseVector(bytesMB),
      style = '-'
    )

    val xaxis1 = new SymbolAxis("", labels)
    xaxis1.setVerticalTickLabels(true)
    p1.plot.setDomainAxis(xaxis1)
    p1.ylabel = "MB Sent"
    p1.title = s"Top $topN IPs by Traffic Volume"

    // Subplot 2: Numero di connessioni
    val p2 = f.subplot(2, 1, 1)
    p2 += plot(
      breeze.linalg.DenseVector(indices),
      breeze.linalg.DenseVector(connections),
      style = '+'
    )

    val xaxis2 = new SymbolAxis("IP [Traffic Class]", labels)
    xaxis2.setVerticalTickLabels(true)
    p2.plot.setDomainAxis(xaxis2)
    p2.ylabel = "Connections"

    println(s"  → top_talkers.png (top $topN)")
    val totalGB = bytesMB.sum / 1024.0
    val totalConn = connections.sum.toLong
    println(f"     Total: ${totalGB}%.2f GB, ${totalConn}%d connections")

    // Mostra top 5
    println("     Top 5:")
    top.take(5).foreach { case (ip, profile) =>
      val mb = profile.total_bytes_sent.toDouble / (1024 * 1024)
      println(f"       $ip%-18s: ${mb}%8.2f MB, ${profile.connection_count}%6d conn [${profile.traffic_class}]")
    }

    saveFigure(f, out, fs)
  }
}