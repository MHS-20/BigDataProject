package project

import scala.io.Source
import java.io.PrintWriter

import scala.io.Source
import java.io.PrintWriter

object CSVLabelCleaner {
  def main(args: Array[String]): Unit = {

    val inputFile = "C:\\Users\\muham\\Desktop\\Coding\\Unibo\\BigData\\BigDataProject\\datasets\\dataset18.csv"
    val outputFile = "C:\\Users\\muham\\Desktop\\Coding\\Unibo\\BigData\\BigDataProject\\datasets\\dataset28.csv"

    try {
      val lines = Source.fromFile(inputFile).getLines().toList
      val writer = new PrintWriter(outputFile)
      lines.zipWithIndex.foreach { case (line, idx) =>
        if (idx == 0) {
          // Process header: extract column names and rename last column
          val columns = line.split("\\s{2,}|,(?=\\S)") // Split by multiple spaces or comma followed by non-space
          val cleanedColumns = columns.dropRight(2) ++ Array("label")
          writer.println(cleanedColumns.mkString(","))
        } else {
          // Process data rows
          val parts = line.split("\\s{2,}") // Split by multiple spaces

          if (parts.length >= 2) {
            // Get all parts except the last two (label and detailed-label)
            val dataParts = parts.dropRight(2)

            // Get the label (second to last part) and clean it
            val label = parts(parts.length - 2)
              .trim
              .toLowerCase
              .replaceAll("[\\s-]+", "") // Remove spaces and hyphens

            writer.println(dataParts.mkString(",") + "," + label)
          } else {
            writer.println(line) // Keep malformed lines as-is
          }
        }
      }

      println(s"Successfully cleaned CSV. Output written to: $outputFile")

    } finally {
      writer.close()
    }
  }
}
