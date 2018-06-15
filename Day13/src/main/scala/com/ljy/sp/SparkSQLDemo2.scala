package com.ljy.sp

import com.ljy.scala.LoggerLevels
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * dataset 练习
  */
object SparkSQLDemo2 {
  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val spark = SparkSession.builder()
      .appName("SparkSQLDemo2")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val df: DataFrame = spark.read.json("hdfs://h1:9000//spark2x/in/day01/people.json")
    df.printSchema()
    val ds: Dataset[Person] = df.as[Person]
    ds.printSchema()
    ds.show()


    println("=============================")

    val dsp: Dataset[Person] = Seq(Person("xiaoming",18,90),Person("xiaoming",21,99)).toDS()
    dsp.printSchema()
    dsp.show()
    println("----------------------")
    dsp.createOrReplaceTempView("t_p")
    spark.sql("select * from t_p").show()


    spark.stop()
  }
}

// 用dataset ,通常都会通过case class 来定义Dataset的数据结构
case class Person(name: String, age: Long, facevalue: Long)
