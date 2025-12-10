package lab104

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import utils._

object WordCount {

  val inputFile = "/datasets/divinacommedia.txt"
  val outputDir = "/output/myFirstWordCount" // The output directory should NOT exist

  def main(args: Array[String]): Unit = {

    // Create a SparkConf object; the configuration settings you put here will override those given in the Run/Debug configuration
    val sparkConf = new SparkConf()
      .setAppName("Word Count")
    //.setMaster("local[4]") // not ideal if you plan to deploy this code to a remote cluster
    val sc = new SparkContext(sparkConf)

    if(args.length == 0){
      println("The first parameter should indicate the deployment mode (\"local\" or \"remote\")")
      return
    }
    val deploymentMode = args(0)

    val myRdd = sc.textFile(Commons.getDatasetPath(deploymentMode, inputFile),4)
    myRdd.
      flatMap(_.split(" ") ).
      map(x => (x,1)).
      reduceByKey(_+_).
      map({case (k,v) => (v,k)}).
      sortByKey(false).
      coalesce(1).
      saveAsTextFile(Commons.getDatasetPath(deploymentMode,outputDir))
  }

}