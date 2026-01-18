package utils

object Config {

  // The local directory containing this repository
  val projectDir :String = "C:\\Users\\muham\\Desktop\\Coding\\Unibo\\Corsi\\BigData\\BigDataProject"

  // The name of the shared bucket on AWS S3 to read datasets (so you don't need to upload them in your bucket)
  val s3sharedBucketName :String = "unibo-bd2526-egallinucci-shared"

  // The name of your bucket on AWS S3
  val s3bucketName :String = "mhs-lab1/datasets"

  // The path to the credentials file for AWS (if you follow instructions, this should not be updated)
  val credentialsPath :String = "/aws_credentials.txt"

  // The path to the dataset
  val dataPath = "C:\\Users\\muham\\Desktop\\Coding\\Unibo\\Corsi\\BigData\\BigDataProject\\datasets"

  // The output paths
  val remoteOutputPath = "s3a://mhs-lab1/output"
  val localOutputPath = "C:\\Users\\muham\\Desktop\\Coding\\Unibo\\Corsi\\BigData\\BigDataProject\\output"

  // The charts paths
  val localChartPath = "C:\\Users\\muham\\Desktop\\Coding\\Unibo\\Corsi\\BigData\\BigDataProject\\charts"
  val remoteChartPath = "s3a://mhs-lab1/charts"

}
