package com.skkodali.bikesharing.regression
import com.amazonaws.services.sagemaker.sparksdk.{IAMRole, RandomNamePolicyFactory}
import com.amazonaws.services.sagemaker.sparksdk.algorithms.LinearLearnerSageMakerEstimator
import com.skkodali.bikesharing.caseclasses.BikeCaseClass
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature._
import com.amazonaws.services.sagemaker.sparksdk.algorithms.LinearLearnerRegressor
import org.apache.spark.ml.linalg.Matrix

/*
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.impurity.Entropy
import org.apache.spark.mllib.tree.impurity.Gini
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import breeze.linalg._
import breeze.numerics._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import java.sql.Timestamp
import java.util.Date
import java.util.Calendar*/

// https://www.veribilimi.co/2018/05/24/apache-spark-2-3-0-onehotencoderestimator-scala-ornek-uygulamasi/
// https://community.hortonworks.com/articles/53903/spark-machine-learning-pipeline-by-example.html
// AWS pyspark - sagemaker documentation : https://media.readthedocs.org/pdf/sagemaker-pyspark/latest/sagemaker-pyspark.pdf
// https://dzone.com/articles/feature-hashing-for-scalable-machine-learning

import org.apache.spark.sql.Dataset
class LinearRegressionForBikeSharing extends java.io.Serializable {

  def ApplyStringIndexer(columnname: String): Unit = {
    val indexer = new StringIndexer()
      .setInputCol(columnname)
      .setOutputCol(columnname + "_ind")
  }
  var categoryLength = 0
  def linearRegressionModel(fieldsForRegression: Dataset[BikeCaseClass],
                            stepSize: Double,
                            numberOfIterations: Int,
                            regressionSparkSession: SparkSession)
  {
    val helperFunctions: HelperFunctions = new HelperFunctions()
    println("Calling getCatoricalAndNumericalFieldsFromCaseClass function....")
    val catogoricalColumnsList =
      helperFunctions.getCatoricalAndNumericalFieldsFromCaseClass._1
    val numericalColumnsList =
      helperFunctions.getCatoricalAndNumericalFieldsFromCaseClass._2.dropRight(1)
    val labelledColumn = numericalColumnsList.last

    println("Categorical columns list ::")
    catogoricalColumnsList.foreach(println)
    println("Numberical columns list ::")
    numericalColumnsList.foreach(println)
    println("Labelled columns list ::")
    labelledColumn.foreach(println)

    //val computeData = fieldsForRegression.map{ eachRecord => LabeledPoint(scala.math.log(helperFunctions.getLabel(eachRecord)),   )}

    println("From fields for regression ::: ")
    fieldsForRegression.show(20)

    val splits = fieldsForRegression.randomSplit(Array(0.9, 0.1))
    val training = splits(0).cache()
    val test = splits(1).cache()

    training.cache()
    test.cache()
    val numTraining = training.count()
    val numTest = test.count()
    println(s"======================  Training: $numTraining, test: $numTest.")

    println("Training data is :: ")
    println(training.show(10))

    println("Test data is ::: ")
    print(test.show(10))

    //var indexers1: Array[StringIndexer] = Array()

    println("All categorical columns are :::: ")
    catogoricalColumnsList.foreach(println)

    /*for (colname <- catogoricalColumnsList)
    {
      val index1 = new StringIndexer().setInputCol(colname).setOutputCol(colname+"_ind")
      println(index1.getOutputCol)
      indexers1 = indexers1 :+ index1
    }*/

    val indexers =
      catogoricalColumnsList.map { colName =>
        new StringIndexer().setInputCol(colName).setOutputCol(colName + "_ind").setHandleInvalid("keep")
      }

    /*
    println("Indexers.....")

    val d:Array[String] = indexers1.map(x=> x.getOutputCol)
    indexers1.map(x=> x.getOutputCol).foreach(println)
    indexers1.foreach(x=> println(x.getOutputCol))
    */


    // In spark < 2.3.0 - Not using in this.
    /*val encoders = indexers.map {
      ind => new OneHotEncoder().setInputCol(ind.getOutputCol).setOutputCol(s"${ind}_vec")

    }*/

    val indexerInputColumns : Array[String] = indexers.map(x=>x.getOutputCol)
    indexerInputColumns.foreach(k => println(k.toString))

    val indexerOutputColumns : Array[String] = indexers.map(iOC => s"${iOC.getOutputCol}_vec")

    val encodersOEE = new OneHotEncoderEstimator().setDropLast(false).setInputCols(indexerInputColumns).setOutputCols(indexerOutputColumns)
    val l = encodersOEE.getOutputCols ++ numericalColumnsList.map(x => x.toString)
    println("Assembler......")
    l.foreach(println)
    val assemblerInputColumns = encodersOEE.getOutputCols ++ numericalColumnsList.map(x => x.toString)
    val assembler = new VectorAssembler().setInputCols(assemblerInputColumns).setOutputCol("features")
    println("Assembler Data......")

    //assembler.transform(training).show(5)


    //scale the features - check the hortonworks link
    val scaler = new StandardScaler().setInputCol("vectorfeatures").setOutputCol("features").setWithStd(true).setWithMean(true)

    val sagemakerIAMRole = "arn:aws:iam::246997141225:role/sagemaker-emr-custom-role"
    val linearLearnerRegressor = new LinearLearnerRegressor(trainingInstanceType = "ml.m4.xlarge", trainingInstanceCount = 1,
                                       endpointInstanceType = "ml.m4.xlarge", endpointInitialInstanceCount = 1,
                                       sagemakerRole = IAMRole(sagemakerIAMRole), namePolicyFactory = new RandomNamePolicyFactory())

    linearLearnerRegressor.setFeatureDim(37)

    //assembler.transform(training).show(5)

    /*The SageMakerEstimator expects an input DataFrame with a column named “features” that holds a Spark ML Vector.
    The estimator also serializes a “label” column of Doubles if present. Other columns are ignored. The dimension of
    this input vector should be equal to the feature dimension given as a hyperparameter. */

    val pipeline = new Pipeline().setStages(indexers ++ Array(encodersOEE,assembler, scaler, linearLearnerRegressor))

    /*var featureTrainingDF = assembler.transform(training)
    featureTrainingDF = featureTrainingDF.select("features","cnt")
    // Renaming the "cnt" column to "label" as sagemaker's algorithms expects "features" and "label" as columns in the dataframes.
    // Other columns from the dataframe will be ignored.
    featureTrainingDF.withColumnRenamed("cnt", "label")
    featureTrainingDF.show(10)*/


    val featureTrainingDF = training.withColumnRenamed("cnt", "label")
    println("Feature training DF is ::: ")
    featureTrainingDF.show()
    //val pipeline = new Pipeline().setStages(indexers)
    val model = pipeline.fit(featureTrainingDF)
    val encodedData = model.transform(featureTrainingDF)
    encodedData.show()

    val featureTestDF = test.withColumnRenamed("cnt","label")
    val testEncoded = model.transform(featureTestDF)
    testEncoded.show()

    /*
    for (colname <- catogoricalColumnsList)
     {
       val index1 = new StringIndexer().setInputCol(colname).setOutputCol(colname+"_ind")
       indexers1 = indexers1 :+ index1
     }
     */
    /*
    if (numberOfCategoricalFeatures > 0)
    {
      val mappings = helperFunctions.getMappingsForCategoricalFeatures(fieldsForRegression,numberOfCategoricalFeatures)
      localMaps = mappings._1
      categoryLength = mappings._2
    }

    val computeData = fieldsInList.map { eachRecord => LabeledPoint(scala.math.log(helperFunctions.getLabel(eachRecord)), helperFunctions.extractFeaturesForAGivenRecord(eachRecord, categoryLength, numberOfCategoricalFeatures, numberOfNumericalFeatures, localMaps)) }
    computeData.saveAsTextFile("resultsLabeledPoint")


    //For Testing

    val vectors = computeData.map { lp => lp.features }
    val matrix = new RowMatrix(vectors)
    val matrixSummary = matrix.computeColumnSummaryStatistics()

    println("Mean Values are : " + matrixSummary.mean)
    println("Variance Values are : " + matrixSummary.variance)

    val firstRecord = computeData.first()
    println("Linear Model  Label : " + firstRecord.label)
    println("Linear Model  Feature Vector : " + firstRecord.features)

    // Scale the features
    val scaler = new StandardScaler(withMean = true, withStd = true).fit(computeData.map(x => x.features))
    val scaledData = computeData.map(x => LabeledPoint(x.label, scaler.transform(Vectors.dense(x.features.toArray))))
    scaledData.cache()
    println("Scaled Data Features: " + scaledData.first.features)


    val splits = scaledData.randomSplit(Array(0.9, 0.1))
    val training = splits(0).cache()
    val test = splits(1).cache()

    training.cache()
    test.cache()
    val numTraining = training.count()
    val numTest = test.count()
    println(s"======================  Training: $numTraining, test: $numTest.")

    println("Training Model  Label : " + training.first.label)
    println("Training Model  Feature Vector : " + training.first.features)

    val algorithm = new LinearRegressionWithSGD().setIntercept(true)
    algorithm.optimizer
      .setNumIterations(numberOfIterations)
      .setStepSize(stepSize)

    val model = algorithm.run(training)
    val lrIntercept = model.intercept
    val lrWeights = model.weights

    println(s"======================  Model intercept: ${model.intercept}, weights: ${model.weights}")

    val trueVsPredicted = test.map {
      tVsP =>
        val prediction = model.predict(tVsP.features)
        (scala.math.exp(tVsP.label), tVsP.features, scala.math.exp(prediction))
      //(scala.math.pow(tVsP.label, 2), tVsP.features, scala.math.pow(prediction, 2))
    }

    val trueVsPredicted1 = test.map {
      tVsP =>
        val prediction = model.predict(tVsP.features)
        (scala.math.exp(tVsP.label), scala.math.exp(prediction))
      //(scala.math.pow(tVsP.label, 2), scala.math.pow(prediction, 2))
    }

    println("Linear Model Predictions : ")
    trueVsPredicted.take(100).foreach({ case (v, f, p) => println(s"Predicted : ${p}, Actual: ${v}, features: ${f}") })

    val predictVsActualOutputFile = "predict_vs_actual.out"
    trueVsPredicted1.saveAsTextFile(predictVsActualOutputFile)

    val MSE = trueVsPredicted.map { case (v, f, p) => scala.math.pow((v - p), 2) }.mean()
    println("======================  training test Mean Squared Error = " + MSE)

    val RMSE = math.sqrt(MSE)
    println("======================  training test Root Mean Squared Error = " + RMSE)

    //f._3 = 3rd tuple which is predicted value - sometimes the predicted value is giving in negative numbers, and log(-ve number) is undefined and when RMSLE is giving output as NaN
    // One example Row
    // Model intercept: 9523.009483157839, weights: [1024.3166117384844,1533.1336200245091,9905.937932075853]
    // Predicted : -696.8090637247824, Actual: 1176.0, features: [-2.2548855197255264,-1.438364504860998,-0.5759067854376712]
    // 9523.009483157839+(1024.3166117384844*-2.2548855197255264)+(1533.1336200245091*-1.438364504860998)+(9905.937932075853*-0.5759067854376712)

    //The below line is to filter out the rows where the predicted value is less 0 (negative).
    //val MSLE = trueVsPredicted.filter(f => f._3 > 0).map { case (v, f, p) => scala.math.pow(((scala.math.log1p(p.toDouble)) - (scala.math.log1p(v.toDouble))), 2) }

    //The below line is to update the predicted value if it is negative value to 0
    val filteredPredicted = trueVsPredicted.map(f => if (f._3 < 0) (f._1, f._2, 1.0) else f)
    val MSLE = filteredPredicted.map { case (v, f, p) => scala.math.pow(((scala.math.log1p(p.toDouble)) - (scala.math.log1p(v.toDouble))), 2) }

    //trueVsPredicted.map { case (v, f, p) => scala.math.pow(((scala.math.log1p(p.toDouble)) - (scala.math.log1p(v.toDouble))), 2) }
    //MSLE.saveAsTextFile("MSLE")
    val RMSLE = math.sqrt(MSLE.mean())
    println("======================  training test Mean Squared Log Error = " + RMSLE)

    val savePath = "saveModelLR.out"
    model.save(regressionSparkContext, savePath)
   */
  }
}
