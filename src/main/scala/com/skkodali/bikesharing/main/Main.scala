package com.skkodali.bikesharing.main

import com.skkodali.bikesharing.parsing.ParseData
import com.skkodali.bikesharing.regression.LinearRegressionForBikeSharing

// spark-submit --master yarn --deploy-mode cluster --class com.skkodali.bikesharing.main.Main s3://skkodali-proserv-us-west-2/ToyotaConnected/BikeSharing-0.0.1-SNAPSHOT.jar Test s3://s-us-west-2/test/hour.csv 0.1 10
// spark-submit --master yarn --deploy-mode client --class com.skkodali.bikesharing.main.Main s3://skkodali-proserv-us-west-2/ToyotaConnected/BikeSharing-0.0.1-SNAPSHOT.jar Test s3://s-v-us-west-2/test/hour.csv 0.1 10

/*
To avoid this error:
Caused by: com.amazonaws.services.sagemaker.model.AmazonSageMakerException: User: arn:aws:sts::222222222:assumed-role/EMR_EC2_DefaultRole/i-0169fcf1ff17070a8 is not authorized to perform: sagemaker:CreateTrainingJob on resource: arn:aws:sagemaker:us-west-2:2222222:training-job/trainingjob-8eddbf528c90-2018-09-11t18-48-19-551 (Service: AmazonSageMaker; Status Code: 400; Error Code: AccessDeniedException; Request ID: b8ed6dfa-e950-447e-a6fa-2ef924ba06a6)
To avoid this ::: Make sure "EMR_EC2_DefaultRole" has sagemaker full access policy is attached as well.


 */
object Main
{
  def main(args: Array[String])
  {
   println("In Mains")
   
    if(args(0).trim().equals("Test"))
        {
          println("In Test")
          var fileName = args(1)
          println("File name is :" + fileName)
          val stepSize = args(2).toDouble
          val numberOfIterations = args(3).toInt
          
          var parsedData: ParseData = new ParseData()
          parsedData.setup()
          val result = parsedData.getParsedData(fileName)
          val fields = result._1
          val sparksession = result._2
          fields.cache()
          val linearRegression: LinearRegressionForBikeSharing = new LinearRegressionForBikeSharing()
          linearRegression.linearRegressionModel(fields, stepSize, numberOfIterations, sparksession)
          
          parsedData.cleanup()
        }
  }
}