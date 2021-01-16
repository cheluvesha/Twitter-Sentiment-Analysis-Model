package com.utilities

import org.apache.spark.sql.SparkSession

object Utility {

  /***
    * Creates SparkSession object
    * @param appName String
    * @return SparkSession
    */
  def createSparkSessionObj(appName: String): SparkSession = {
    val sparkSession = SparkSession
      .builder()
      .appName(appName)
      .master("local[*]")
      .getOrCreate()
    sparkSession
  }
}
