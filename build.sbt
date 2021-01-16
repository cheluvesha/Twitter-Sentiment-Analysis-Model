name := "Twitter_SentimentAnalysis_Model"

version := "0.1"

scalaVersion := "2.12.10"

scapegoatVersion in ThisBuild := "1.3.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.0"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.0.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test

libraryDependencies += "jp.co.bizreach" %% "aws-s3-scala" % "0.0.15"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.2.1"

libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.2.1"
