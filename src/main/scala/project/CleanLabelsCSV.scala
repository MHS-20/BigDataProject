package project

import scala.io.Source
import java.io.PrintWriter

object CSVLabelCleaner {

  def main(args: Array[String]): Unit = {

    val inputFile = "C:\\Users\\muham\\Desktop\\Coding\\Unibo\\Corsi\\BigData\\BigDataProject\\datasets\\dataset18.csv"
    val outputFile = "C:\\Users\\muham\\Desktop\\Coding\\Unibo\\Corsi\\BigData\\BigDataProject\\datasets\\dataset18-2.csv"

    val source = Source.fromFile(inputFile)
    val writer = new PrintWriter(outputFile)

    try {
      val lines = source.getLines()
      var isHeader = true

      for (line <- lines) {
        if (isHeader) {
          // Process header
          val columns = line.split("\\s{2,}|,(?=\\S)") // Split by multiple spaces or comma followed by non-space
          val cleanedColumns = columns.dropRight(2) ++ Array("label")
          writer.println(cleanedColumns.mkString(","))
          isHeader = false
        } else {
          // Process data rows
          val parts = line.split("\\s{2,}") // Split by multiple spaces

          if (parts.length >= 2) {
            val dataParts = parts.dropRight(2)

            val label = parts(parts.length - 2)
              .trim
              .toLowerCase
              .replaceAll("[\\s-]+", "") // remove spaces and hyphens

            writer.println(dataParts.mkString(",") + "," + label)
          } else {
            // Keep malformed lines as-is
            writer.println(line)
          }
        }
      }

      println(s"Successfully cleaned CSV. Output written to: $outputFile")

    } finally {
      writer.close()
      source.close()
    }
  }
}
