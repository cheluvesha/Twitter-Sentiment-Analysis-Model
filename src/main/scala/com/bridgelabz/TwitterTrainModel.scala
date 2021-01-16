package com.bridgelabz

import java.io.FileNotFoundException

import com.awsUtils.AWSConfiguration
import com.utilities.Utility
import org.apache.log4j.Logger
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.{
  BinaryClassificationEvaluator,
  MulticlassClassificationEvaluator
}
import org.apache.spark.ml.feature._
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.{DataFrame, SparkSession}

object TwitterTrainModel {

  val sparkSession: SparkSession = Utility.createSparkSessionObj("Train Model")
  val logger: Logger = Logger.getLogger(getClass.getName)

  /***
    * Reads data from S3 bucket and creates DataFrame
    * @param s3Path String
    * @return DataFrame
    */
  def readDataFromS3AndCreateDF(s3Path: String): DataFrame = {
    try {
      logger.info("Reading data from aws S3")
      val tweetsRawDF = sparkSession.read
        .option("header", value = true)
        .option("inferSchema", value = true)
        .csv(s3Path)
      tweetsRawDF
    } catch {
      case accessDeniedEx: java.nio.file.AccessDeniedException =>
        logger.error(accessDeniedEx.printStackTrace())
        throw new Exception("Invalid AWS Credentials!!!")
      case fileNotFoundEx: FileNotFoundException =>
        logger.error(fileNotFoundEx.printStackTrace())
        throw new Exception("Specified file from S3 is not exist")
    }
  }

  /***
    * Pre-processing the raw tweet data
    * @param tweetsRawDF DataFrame
    * @return DataFrame
    */
  def cleanTweetRawDF(tweetsRawDF: DataFrame): DataFrame = {
    try {
      logger.info("Started preprocessing the tweet raw data")
      val cleanedTweetDF =
        tweetsRawDF.withColumn(
          "tweet",
          regexp_replace(tweetsRawDF.col("tweet"), "[^A-Za-z]", " ")
        )
      cleanedTweetDF
    } catch {
      case sqlAnalysisException: org.apache.spark.sql.AnalysisException =>
        logger.error(sqlAnalysisException.printStackTrace())
        throw new Exception("Unable to process SQL operation on DataFrame")
    }
  }

  /***
    * Training a model by splitting the DataFrame and applying logistic regression by feature extraction
    * and creating pipeline
    * @param cleanTweetDF DataFrame
    * @param pathToSaveModel String
    * @return DataFrame
    */
  def trainModelForTweetData(
      cleanTweetDF: DataFrame,
      pathToSaveModel: String
  ): DataFrame = {
    logger.info("Started training the model with stages")
    val Array(trainingData, testData) =
      cleanTweetDF.randomSplit(Array(0.8, 0.2), seed = 5043)
    trainingData.cache()
    // Tokenizer converts the input string into lowercase and then splits the string with whitespaces into individual tokens
    val tokenizer = new Tokenizer().setInputCol("tweet").setOutputCol("words")
    // removing the non sentiment words from the tweet text
    val remover = new StopWordsRemover()
      .setInputCol("words")
      .setOutputCol("filtered")
      .setCaseSensitive(false)
    // hashing trick to achieve word frequency
    val hashingTF =
      new HashingTF()
        .setNumFeatures(10000)
        .setInputCol("filtered")
        .setOutputCol("rawFeatures")
    val idf = new IDF()
      .setInputCol("rawFeatures")
      .setOutputCol("features")
      .setMinDocFreq(0)
    val logisticRegression =
      new LogisticRegression()
        .setRegParam(0.01)
        .setThreshold(0.5)
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, remover, hashingTF, idf, logisticRegression))
    val model = pipeline.fit(trainingData)
    // writing the model to specified path
    model.write
      .overwrite()
      .save(pathToSaveModel)
    val prediction = model.transform(testData)
    prediction
  }

  /***
    * Evaluates model output for a metric
    * @param predictionDF DataFrame
    */
  def evaluatingMetricForThePrediction(predictionDF: DataFrame): Unit = {
    logger.info("printing the metrics for the predictions")
    val evaluator = new BinaryClassificationEvaluator("areaUnderRoc")
    println("Area under the roc curve : " + evaluator.evaluate(predictionDF))
    val eval = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
    println(
      s"Accuracy : ${eval.setMetricName("accuracy").evaluate(predictionDF)}"
    )
    println(
      s"Precision : ${eval.setMetricName("weightedPrecision").evaluate(predictionDF)}"
    )
    println(
      s"Recall : ${eval.setMetricName("weightedRecall").evaluate(predictionDF)}"
    )
    println(s"F1 : ${eval.setMetricName("f1").evaluate(predictionDF)}")
  }

  // entry point to an application
  def main(args: Array[String]): Unit = {
    val s3Path = "s3a://twitter-historic-data/*.csv"
    val pathToSaveModel = "Model"
    val status = AWSConfiguration.connectToS3(
      sparkSession.sparkContext,
      System.getenv("AWS_ACCESS_KEY"),
      System.getenv("AWS_SECRET_KEY")
    )
    logger.info("AWS configuration successful if it is true: " + status)
    val tweetsRawDF = readDataFromS3AndCreateDF(s3Path)
    tweetsRawDF.cache()
    tweetsRawDF.printSchema()
    tweetsRawDF.show()
    val cleanTweeTDF = cleanTweetRawDF(tweetsRawDF)
    cleanTweeTDF.show()
    val predictionDF = trainModelForTweetData(cleanTweeTDF, pathToSaveModel)
    predictionDF.show()
    predictionDF.cache()
    evaluatingMetricForThePrediction(predictionDF)
  }

}
