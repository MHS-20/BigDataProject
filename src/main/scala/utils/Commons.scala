package utils

import org.apache.spark.SparkContext

import java.io.InputStream

object Commons {
  object DeploymentMode extends Enumeration {
    type DeploymentMode = Value
    val local, remote = Value
  }

  import DeploymentMode._

  def initializeSparkContext(deploymentMode: String, sc: SparkContext): Unit = {
    if(deploymentMode == remote){
      val stream: InputStream = getClass.getResourceAsStream(Config.credentialsPath)
      val lines = scala.io.Source.fromInputStream( stream ).getLines.toList

      sc.hadoopConfiguration.set("fs.s3a.fast.upload", "true")
      sc.hadoopConfiguration.set("fs.s3a.fast.upload.buffer", "bytebuffer")

      sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", lines(0))
      sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", lines(1))
    }
  }

  def getDatasetPath(deploymentMode: String, localPath: String, remotePath: String): String = {
    if(deploymentMode == "local"){
      return "file:///" + Config.dataPath.replace("\\", "/") + "/" + localPath
    }
    else if(deploymentMode == "sharedRemote"){
      return "s3a://" + Config.s3sharedBucketName + "/" + remotePath
    }
    else{
      return "s3a://" + Config.s3bucketName + "/" + remotePath
    }
  }

  def getDatasetPath(deploymentMode: String, path: String): String = {
    return getDatasetPath(deploymentMode, path, path)
  }

  def getOutputPath(deploymentMode: String): String = {
    if(deploymentMode == "local"){
      return Config.localOutputPath
    }
    else{
      return Config.remoteOutputPath
    }
  }
}