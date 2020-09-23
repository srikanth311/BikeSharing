package com.skkodali.bikesharing.configuration

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class ConfigurationFactory 
{
  def getSparkSession() : SparkSession  =
  {
    val spark = SparkSession.builder().appName("Bike Sharing Using Spark and Sagemaker")
      //.config("spark.hadoop.spark.sql.parquet.output.committer.class","com.netflix.bdp.s3.S3PartitionedOutputCommitter")
      .config("spark.serizalizer","org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "256m")
      .config("spark.akka.frameSize","128")
      .enableHiveSupport()
      .getOrCreate()

    return spark

  }
}