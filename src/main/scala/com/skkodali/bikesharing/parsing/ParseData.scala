package com.skkodali.bikesharing.parsing

import com.skkodali.bikesharing.configuration.ConfigurationFactory
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.skkodali.bikesharing.caseclasses.BikeCaseClass
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.DoubleType

class ParseData 
{
  var spark : SparkSession = null
  def setup()
  {
    println("In Setup ")
    val configurationFactory = new ConfigurationFactory()
    spark = configurationFactory.getSparkSession()

  }
  
  def getParsedData(fileName : String) : (Dataset[BikeCaseClass], SparkSession) =
  {
    /* val rawData = spark.sparkContext.textFile(fileName)
    rawData.cache()
    val parseRawInfo : ParseRawInfo = new ParseRawInfo()
    var parsedData = rawData.map { line => parseRawInfo.parse(line) }*/

    /*
    Pass schema while reading the file, otherwise getting this below error
    Exception in thread "main" org.apache.spark.sql.AnalysisException: Cannot up cast `instant` from string to double as it may truncate
    The type path of the target object is:
    - field (class: "scala.Double", name: "instant")
    - root class: "com.skkodali.bikesharing.caseclasses.BikeCaseClass"
    You can either add an explicit cast to the input data or choose a higher precision type of the field in the target object;
    */

    val bikeschema = Encoders.product[BikeCaseClass].schema
    //val df : DataFrame = spark.read.format("csv").option("header", "true").schema(bikeschema).load(fileName)

    val df : DataFrame = spark.read.format("csv").option("header", "true").load(fileName)

    // To avoid error: stable identifier required, but ParseData.this.spark.implicits found. Since spark is declared as "var"
    val localsparksession = spark
    import localsparksession.implicits._


    println("In parsed data .... Dataframe")
    df.show(20)

    val ds = df.withColumn("instant",'instant.cast(DoubleType))
      .withColumn("season",'season.cast(DoubleType))
      .withColumn("yr",'yr.cast(DoubleType))
      .withColumn("mnth",'mnth.cast(DoubleType))
      .withColumn("hr",'hr.cast(DoubleType))
      .withColumn("holiday",'holiday.cast(DoubleType))
      .withColumn("weekday",'weekday.cast(DoubleType))
      .withColumn("workingDay",'workingDay.cast(DoubleType))
      .withColumn("weathersit",'weathersit.cast(DoubleType))
      .withColumn("temp",'temp.cast(DoubleType))
      .withColumn("atemp",'atemp.cast(DoubleType))
      .withColumn("hum",'hum.cast(DoubleType))
      .withColumn("windspeed",'windspeed.cast(DoubleType))
      .withColumn("casual",'casual.cast(DoubleType))
      .withColumn("registered",'registered.cast(DoubleType))
      .withColumn("cnt",'cnt.cast(DoubleType))
              .as[BikeCaseClass]

    println("In get parsed data.... ")
    ds.show(20)
    return (ds, spark)
  }
  
  def cleanup()
  {
    spark.sparkContext.stop()
  }
}