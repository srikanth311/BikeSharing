package com.skkodali.bikesharing.regression

import org.apache.spark.rdd.RDD
import com.skkodali.bikesharing.caseclasses.BikeCaseClass
import com.skkodali.bikesharing.printing.Display
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
//import breeze.linalg.DenseVector
import org.apache.spark.SparkContext
//import org.apache.spark.ml.linalg.Vectors
//import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{DataFrame, Dataset}
import scala.reflect.runtime.universe._

class HelperFunctions extends java.io.Serializable {
  def convertBikeClassDataSetToList(
      fields: Dataset[BikeCaseClass],
      sparkSession: SparkSession): Dataset[List[Any]] = {
    import sparkSession.implicits._
    val listFields = fields.map { line =>
      var rowValues = List[Any]()
      //Always return String Type columns first.
      rowValues = List(
        line.color,
        line.season,
        line.yr,
        line.mnth,
        line.hr,
        line.holiday,
        line.weekday,
        line.workingDay,
        line.weathersit,
        line.temp,
        line.atemp,
        line.hum,
        line.windspeed,
        line.casual,
        line.registered,
        line.cnt
      )
      rowValues
    }
    return listFields
  }

  // Different way to find out categorical and numberical features - not using it
  def getCatogoricalAndNumericalFields(ds: Dataset[BikeCaseClass]) {
    val fieldNames = ds.schema.fieldNames.foreach(x => println(x))
    val filedTypes = ds.schema.fields.foreach(y => println(y))
    ds.schema.fields.foreach(y => println(y.dataType.toString, y.name.toString))
    var numberOfCategoricalFeatures = 0
    var numberOfNumericalFeatures = 0

    ds.schema.fields.foreach { y =>
      y.dataType.toString match {
        case "StringType" =>
          numberOfCategoricalFeatures = numberOfCategoricalFeatures + 1
        case "DoubleType" =>
          numberOfNumericalFeatures = numberOfNumericalFeatures + 1
      }
    }
    println(
      "Number of Categorical features are : " + numberOfCategoricalFeatures)
    println("Number of numerical features are : " + numberOfNumericalFeatures)
  }

  def getCatoricalAndNumericalFieldsFromCaseClass
    : (Array[String], Array[String]) = {
    var categoricalColumns = ArrayBuffer[String]()
    var numericalColumns = ArrayBuffer[String]()

    classOf[BikeCaseClass].getDeclaredFields
      .foreach {
        //case t if t.getType == classOf[Integer] => println("Int " + t.getName)
        case s if s.getType == classOf[String] =>
          categoricalColumns += s.getName.toString
        case d if d.getType == classOf[Double] =>
          numericalColumns += d.getName.toString
        case _ => println("Unknown")
      }
    /*println("Categorical columns :::")
    categoricalColumns.foreach(println)
    println("Numerical columns :::")
    numericalColumns.foreach(println)*/
    return (categoricalColumns.toArray, numericalColumns.toArray)
  }

  def findNumberOfCategoricalAndNumericalFields(
      fieldsInList: List[Any]): (Int, Int) = {
    var numberOfCategoricalFeatures = 0
    var numberOfNumericalFeatures = 0

    val categories = fieldsInList.collect {
      case s: String =>
        numberOfCategoricalFeatures = numberOfCategoricalFeatures + 1
      case d: Double =>
        numberOfNumericalFeatures = numberOfNumericalFeatures + 1
    }

    val numberOfFeaturesTuple =
      (numberOfCategoricalFeatures, numberOfNumericalFeatures)
    return numberOfFeaturesTuple
  }

  def getLabel(row: BikeCaseClass): Double =
  {
    return row.cnt.asInstanceOf[Double]
  }

  /* def getMappingsForCategoricalFeatures(fieldsForRegression: RDD[BikeCaseClass], numberOfCategoricalFeatures: Int): (Array[Map[Any, Long]], Int) =
    {

      println("Number of Categorical Featrures are : " + numberOfCategoricalFeatures)
      var maps: Array[Map[Any, Long]] = new Array[Map[Any, Long]](numberOfCategoricalFeatures)
      var categoryLength = 0
      val fieldsInList = convertBikeClassDataSetToList(fieldsForRegression)

      for (i <- 0 to numberOfCategoricalFeatures - 1) {
        maps(i) = fieldsInList.map(columns => columns(i)).distinct().zipWithIndex().collect().toMap
        categoryLength = categoryLength + maps(i).size
      }
      println("In getMappingsForCategoricalFeatures : CategoricalFeatures Length is : " + categoryLength)
      return (maps, categoryLength)
    }

  def getLabel(row: List[Any]): Double =
    {
      return row.last.asInstanceOf[Double]
    }
  def extractFeaturesForAGivenRecord(row: List[Any], size: Int, categoryFeaturesSize: Int, numericalFeaturesSize: Int, mappings: Array[Map[Any, Long]]): Vector =
    {
      val disp: Display = new Display()
      val categoryVector = DenseVector.zeros[Double](size).toArray
      var i = 0
      var step = 0
      val index = 0

      if (categoryFeaturesSize > 0) {
        for (j <- 0 to categoryFeaturesSize - 1) {
          val m = mappings(i)
          val index = m(row(j))
          val newIndex = (index + step).toInt
          categoryVector(newIndex) = 1
          i = i + 1
          step = step + m.size
        }
      }

      val numericVector = new Array[Double](numericalFeaturesSize) //Last column passed in the row is Label which we are going to predict.
      var l = categoryFeaturesSize

      for (k <- 0 to numericalFeaturesSize - 1) {
        val s = row.lift(l).get
        numericVector(k) = s.asInstanceOf[Double]
        l = l + 1
      }

      var combineFeatures: Array[Double] = new Array(2)

      if (categoryFeaturesSize > 0) {
        combineFeatures = categoryVector ++ numericVector
        disp.displayArrays(combineFeatures)
      } else {
        combineFeatures = numericVector
        disp.displayArrays(combineFeatures)
      }
      return Vectors.dense(combineFeatures)

    }

  def withAdditionalColumns(originalColName : String, origLabel: String, origLabelIndex: Int,df : DataFrame, sparkContext: SparkContext) : (String, DataFrame) =
  {
    val newColName = originalColName + "_" + origLabel
    val broadcastedOriginalLabelIndex = sparkContext.broadcast(origLabelIndex)

    val encoder: (Double => Double) = (value : Double) => {
      if (value.toInt == broadcastedOriginalLabelIndex.value) { 1.0 }
      else { 0.0 }
    }
    val sqlfunc = udf(encoder)
    (newColName, df.withColumn(newColName, sqlfunc(col(originalColName + "_indexed"))))
  }*/
}
