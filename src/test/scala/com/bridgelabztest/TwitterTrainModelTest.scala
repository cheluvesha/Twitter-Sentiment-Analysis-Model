package com.bridgelabztest

import com.awsUtils.AWSConfiguration
import com.bridgelabz.TwitterTrainModel
import com.utilities.Utility
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.FunSuite

class TwitterTrainModelTest extends FunSuite {
  val sparkSession: SparkSession = Utility.createSparkSessionObj("Test App")
  val s3Path = "s3a://test1-tweet-ss/csv/*.csv"
  val wrongS3Path = "s3a://wrong/*.csv"
  var s3DF: DataFrame = _
  val cleanedTestData =
    " user when a father is dysfunctional and is so selfish he drags his kids into his dysfunction     run"

  test("givenWhenS3PathItHasToReadDataAndCreateDataFrame") {
    val status = AWSConfiguration.connectToS3(
      sparkSession.sparkContext,
      System.getenv("AWS_ACCESS_KEY"),
      System.getenv("AWS_SECRET_KEY")
    )
    s3DF = TwitterTrainModel.readDataFromS3AndCreateDF(s3Path)
    assert(status === true)
    assert(s3DF.count() > 0)
  }
  test("givenWhenWrongS3PathItHasToThrowAnException") {
    val thrown = intercept[Exception] {
      AWSConfiguration.connectToS3(
        sparkSession.sparkContext,
        System.getenv("AWS_ACCESS_KEY"),
        System.getenv("AWS_SECRET_KEY")
      )
      TwitterTrainModel.readDataFromS3AndCreateDF(wrongS3Path)
    }
    assert(thrown.getMessage === "Specified file from S3 is not exist")
  }

  test("givenNullFieldsToSetS3HadoopPropertiesMustThrownAnException") {
    val thrown = intercept[Exception] {
      AWSConfiguration.connectToS3(
        sparkSession.sparkContext,
        null,
        null
      )
    }
    assert(thrown.getMessage === "Hadoop AWS properties are not valid")
  }

  test("givenRawDataDFToCleanAndOutputMustBeEqualAsExpected") {
    val cleanedDF = TwitterTrainModel.cleanTweetRawDF(s3DF)
    var cleanedData: String = null
    cleanedDF
      .take(1)
      .foreach(data => {
        cleanedData = data.getString(2)
      })
    println(cleanedData)
    println(cleanedTestData)
    assert(cleanedData === cleanedTestData)
  }
  test("givenRawDataDFToCleanAndOutputMustNotBeEqualAsExpected") {
    val cleanedDF = TwitterTrainModel.cleanTweetRawDF(s3DF)
    var cleanedData: String = null
    cleanedDF
      .take(1)
      .foreach(data => {
        cleanedData = data.getString(2)
      })
    assert(cleanedData != "")
  }
}
