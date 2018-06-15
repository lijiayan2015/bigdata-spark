package com.ljy.scala

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkSQL {
  def main(args: Array[String]): Unit = {
   /* val ss: SparkSession = SparkSession
      .builder()
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()
    val df: DataFrame = ss.read.json("hdfs://h1:9000/spark2x/in/day01/people.json")
    val dp: Dataset[Person] = df.as[Person]
    dp.show()
    dp.printSchema()
    ss.stop()*/
  }
}


case class Person(name: String, age: Long, facevalue: Long)
